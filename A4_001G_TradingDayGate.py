from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

TZ = ZoneInfo("Asia/Taipei")
TWSE_HOLIDAY_API = "https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule"
OUTPUT_PATH = Path(os.getenv("A4_MARKET_GATE_OUTPUT", "output/mis/market_gate.json"))
REQUEST_TIMEOUT = max(2.0, float(os.getenv("A4_MARKET_GATE_TIMEOUT", "5")))
OVERRIDE_DATE = os.getenv("A4_MARKET_DATE", "").strip()


@dataclass(frozen=True)
class MarketGate:
    market_date: str
    market_status: str
    should_run: bool
    calendar_verified: bool
    reason: str
    source: str
    calendar_name: str | None = None
    calendar_description: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "market_date": self.market_date,
            "market_status": self.market_status,
            "should_run": self.should_run,
            "calendar_verified": self.calendar_verified,
            "reason": self.reason,
            "source": self.source,
            "calendar_name": self.calendar_name,
            "calendar_description": self.calendar_description,
            "checked_at": datetime.now(TZ).isoformat(),
        }


def roc_date_key(day: date) -> str:
    roc_year = day.year - 1911
    if roc_year <= 0:
        raise ValueError(f"unsupported Gregorian year: {day.year}")
    return f"{roc_year:03d}{day.month:02d}{day.day:02d}"


def is_explicit_trading_day(name: str) -> bool:
    compact = name.replace(" ", "")
    return "開始交易日" in compact or "最後交易日" in compact


def classify_from_calendar(day: date, rows: list[dict[str, Any]]) -> MarketGate:
    day_iso = day.isoformat()
    if day.weekday() >= 5:
        return MarketGate(
            market_date=day_iso,
            market_status="MARKET_CLOSED",
            should_run=False,
            calendar_verified=True,
            reason="WEEKEND",
            source="TWSE_OFFICIAL_CALENDAR",
        )

    key = roc_date_key(day)
    matches = [row for row in rows if str(row.get("Date", "")).strip() == key]
    if not matches:
        return MarketGate(
            market_date=day_iso,
            market_status="MARKET_OPEN",
            should_run=True,
            calendar_verified=True,
            reason="NORMAL_TRADING_WEEKDAY",
            source="TWSE_OFFICIAL_CALENDAR",
        )

    trading_match = next(
        (row for row in matches if is_explicit_trading_day(str(row.get("Name", "")))), None
    )
    if trading_match is not None:
        return MarketGate(
            market_date=day_iso,
            market_status="MARKET_OPEN",
            should_run=True,
            calendar_verified=True,
            reason="EXPLICIT_TRADING_DAY",
            source="TWSE_OFFICIAL_CALENDAR",
            calendar_name=str(trading_match.get("Name", "")),
            calendar_description=str(trading_match.get("Description", "")),
        )

    row = matches[0]
    return MarketGate(
        market_date=day_iso,
        market_status="MARKET_CLOSED",
        should_run=False,
        calendar_verified=True,
        reason="OFFICIAL_MARKET_CLOSURE",
        source="TWSE_OFFICIAL_CALENDAR",
        calendar_name=str(row.get("Name", "")),
        calendar_description=str(row.get("Description", "")),
    )


def fetch_calendar() -> list[dict[str, Any]]:
    response = requests.get(
        TWSE_HOLIDAY_API,
        timeout=REQUEST_TIMEOUT,
        headers={
            "User-Agent": "QTS-A4-TradingDayGate/1.0",
            "Accept": "application/json",
        },
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("TWSE holiday calendar response is empty or invalid")
    return [row for row in payload if isinstance(row, dict)]


def determine_market_gate(day: date) -> MarketGate:
    if day.weekday() >= 5:
        return classify_from_calendar(day, [])
    try:
        rows = fetch_calendar()
        return classify_from_calendar(day, rows)
    except Exception as exc:
        return MarketGate(
            market_date=day.isoformat(),
            market_status="CALENDAR_UNVERIFIED",
            should_run=True,
            calendar_verified=False,
            reason=f"FAIL_OPEN_WEEKDAY: {type(exc).__name__}: {exc}",
            source="TWSE_OFFICIAL_CALENDAR",
        )


def target_date() -> date:
    if OVERRIDE_DATE:
        return date.fromisoformat(OVERRIDE_DATE)
    return datetime.now(TZ).date()


def main() -> int:
    gate = determine_market_gate(target_date())
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(gate.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(gate.as_dict(), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        fallback = {
            "market_date": target_date().isoformat(),
            "market_status": "CALENDAR_UNVERIFIED",
            "should_run": True,
            "calendar_verified": False,
            "reason": f"FATAL_FAIL_OPEN_WEEKDAY: {type(exc).__name__}: {exc}",
            "source": "TWSE_OFFICIAL_CALENDAR",
            "checked_at": datetime.now(TZ).isoformat(),
        }
        OUTPUT_PATH.write_text(json.dumps(fallback, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(fallback, ensure_ascii=False))
        sys.exit(0)
