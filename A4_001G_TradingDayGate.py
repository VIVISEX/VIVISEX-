from __future__ import annotations

import html
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

TZ = ZoneInfo("Asia/Taipei")
TWSE_HOLIDAY_API = "https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule"
DGPA_DAILY_URL = "https://www.dgpa.gov.tw/typh/daily/nds.html"
OUTPUT_PATH = Path(os.getenv("A4_MARKET_GATE_OUTPUT", "output/mis/market_gate.json"))
REQUEST_TIMEOUT = max(2.0, float(os.getenv("A4_MARKET_GATE_TIMEOUT", "5")))
OVERRIDE_DATE = os.getenv("A4_MARKET_DATE", "").strip()
USER_AGENT = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36 QTS-A4-MarketGate/2.0"

TAIPEI_DISTRICTS = (
    "松山區", "信義區", "大安區", "中山區", "中正區", "大同區",
    "萬華區", "文山區", "南港區", "內湖區", "士林區", "北投區",
)


class _TextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        text = data.strip()
        if text:
            self.parts.append(text)

    def text(self) -> str:
        return " ".join(self.parts)


@dataclass(frozen=True)
class EmergencyCheck:
    verified: bool
    market_closed: bool
    reason: str
    source: str = "DGPA_OFFICIAL_DAILY_CLOSURE"
    page_date: str | None = None
    detail: str | None = None


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
    emergency_verified: bool = False
    emergency_source: str | None = None
    emergency_reason: str | None = None
    emergency_page_date: str | None = None
    emergency_detail: str | None = None

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
            "emergency_verified": self.emergency_verified,
            "emergency_source": self.emergency_source,
            "emergency_reason": self.emergency_reason,
            "emergency_page_date": self.emergency_page_date,
            "emergency_detail": self.emergency_detail,
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
            emergency_verified=True,
            emergency_source="NOT_REQUIRED",
            emergency_reason="WEEKEND",
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
        emergency_verified=True,
        emergency_source="NOT_REQUIRED",
        emergency_reason="OFFICIAL_MARKET_CLOSURE",
    )


def fetch_calendar() -> list[dict[str, Any]]:
    response = requests.get(
        TWSE_HOLIDAY_API,
        timeout=REQUEST_TIMEOUT,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise RuntimeError("TWSE holiday calendar response is empty or invalid")
    return [row for row in payload if isinstance(row, dict)]


def _html_to_text(raw_html: str) -> str:
    parser = _TextExtractor()
    parser.feed(raw_html)
    text = html.unescape(parser.text())
    return re.sub(r"\s+", " ", text).strip()


def _extract_dgpa_page_date(text: str) -> date | None:
    patterns = (
        r"(?P<y>\d{2,3})\s*年\s*(?P<m>\d{1,2})\s*月\s*(?P<d>\d{1,2})\s*日\s*天然災害停止上班(?:及|、)?上課情形",
        r"(?P<y>\d{2,3})\s*年\s*(?P<m>\d{1,2})\s*月\s*(?P<d>\d{1,2})\s*日.*?停止上班.*?上課情形",
    )
    for pattern in patterns:
        match = re.search(pattern, text)
        if not match:
            continue
        y = int(match.group("y"))
        m = int(match.group("m"))
        d = int(match.group("d"))
        year = y + 1911 if y < 1911 else y
        try:
            return date(year, m, d)
        except ValueError:
            continue
    return None


def _taipei_snippet(text: str, radius: int = 260) -> str | None:
    positions = [p for p in (text.find("臺北市"), text.find("台北市")) if p >= 0]
    if not positions:
        return None
    pos = min(positions)
    return text[max(0, pos - 80): pos + radius]


def classify_emergency_html(day: date, raw_html: str) -> EmergencyCheck:
    text = _html_to_text(raw_html)
    if not text:
        return EmergencyCheck(False, False, "DGPA_EMPTY_PAGE")

    page_day = _extract_dgpa_page_date(text)
    page_day_iso = page_day.isoformat() if page_day else None

    if page_day is not None and page_day != day:
        return EmergencyCheck(
            verified=True,
            market_closed=False,
            reason="NO_CURRENT_DAY_EMERGENCY_NOTICE",
            page_date=page_day_iso,
            detail="DGPA latest notice is for another date",
        )

    if "無停班停課訊息" in text or "無停止上班上課訊息" in text:
        return EmergencyCheck(
            verified=True,
            market_closed=False,
            reason="DGPA_NO_SUSPENSION_NOTICE",
            page_date=page_day_iso,
        )

    snippet = _taipei_snippet(text)
    if snippet is None:
        if page_day == day:
            return EmergencyCheck(
                verified=True,
                market_closed=False,
                reason="DGPA_NO_TAIPEI_CLOSURE_ROW",
                page_date=page_day_iso,
            )
        return EmergencyCheck(False, False, "DGPA_PAGE_DATE_UNPARSEABLE", page_date=page_day_iso)

    compact = re.sub(r"\s+", "", snippet)
    if "照常上班" in compact:
        return EmergencyCheck(
            verified=True,
            market_closed=False,
            reason="TAIPEI_NORMAL_WORK",
            page_date=page_day_iso,
            detail=snippet,
        )

    if "部分地區" in compact or any(district in compact for district in TAIPEI_DISTRICTS):
        return EmergencyCheck(
            verified=True,
            market_closed=False,
            reason="TAIPEI_PARTIAL_AREA_CLOSURE_MARKET_OPEN",
            page_date=page_day_iso,
            detail=snippet,
        )

    has_stop_work = "停止上班" in compact
    afternoon_only = bool(
        re.search(r"(?:下午|中午過後|午後).{0,20}停止上班", compact)
        and not re.search(r"(?:上午|全日|全天|今天|今日).{0,12}停止上班", compact)
    )
    morning_or_full_day = bool(
        re.search(r"(?:上午|全日|全天|今天|今日).{0,20}停止上班", compact)
        or compact.startswith("臺北市停止上班")
        or compact.startswith("台北市停止上班")
    )

    if has_stop_work and afternoon_only:
        return EmergencyCheck(
            verified=True,
            market_closed=False,
            reason="TAIPEI_AFTERNOON_ONLY_CLOSURE_MARKET_OPEN",
            page_date=page_day_iso,
            detail=snippet,
        )

    if has_stop_work and morning_or_full_day:
        return EmergencyCheck(
            verified=True,
            market_closed=True,
            reason="TAIPEI_FULL_OR_MORNING_WORK_SUSPENSION",
            page_date=page_day_iso,
            detail=snippet,
        )

    if has_stop_work:
        return EmergencyCheck(
            verified=False,
            market_closed=False,
            reason="TAIPEI_SUSPENSION_WORDING_AMBIGUOUS_FAIL_OPEN",
            page_date=page_day_iso,
            detail=snippet,
        )

    return EmergencyCheck(
        verified=True,
        market_closed=False,
        reason="TAIPEI_NO_WORK_SUSPENSION",
        page_date=page_day_iso,
        detail=snippet,
    )


def fetch_emergency_check(day: date) -> EmergencyCheck:
    try:
        response = requests.get(
            DGPA_DAILY_URL,
            timeout=REQUEST_TIMEOUT,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "zh-TW,zh;q=0.9",
                "Cache-Control": "no-cache",
            },
        )
        response.raise_for_status()
        response.encoding = response.apparent_encoding or response.encoding
        return classify_emergency_html(day, response.text)
    except Exception as exc:
        return EmergencyCheck(
            verified=False,
            market_closed=False,
            reason=f"DGPA_UNAVAILABLE_FAIL_OPEN: {type(exc).__name__}: {exc}",
        )


def determine_market_gate(day: date) -> MarketGate:
    if day.weekday() >= 5:
        return classify_from_calendar(day, [])

    try:
        rows = fetch_calendar()
        calendar_gate = classify_from_calendar(day, rows)
    except Exception as exc:
        calendar_gate = MarketGate(
            market_date=day.isoformat(),
            market_status="CALENDAR_UNVERIFIED",
            should_run=True,
            calendar_verified=False,
            reason=f"FAIL_OPEN_WEEKDAY: {type(exc).__name__}: {exc}",
            source="TWSE_OFFICIAL_CALENDAR",
        )

    if not calendar_gate.should_run:
        return calendar_gate

    emergency = fetch_emergency_check(day)
    if emergency.market_closed:
        return MarketGate(
            market_date=day.isoformat(),
            market_status="MARKET_CLOSED_EMERGENCY",
            should_run=False,
            calendar_verified=calendar_gate.calendar_verified,
            reason="EMERGENCY_MARKET_CLOSURE",
            source="TWSE_CALENDAR_PLUS_DGPA",
            calendar_name=calendar_gate.calendar_name,
            calendar_description=calendar_gate.calendar_description,
            emergency_verified=emergency.verified,
            emergency_source=emergency.source,
            emergency_reason=emergency.reason,
            emergency_page_date=emergency.page_date,
            emergency_detail=emergency.detail,
        )

    return MarketGate(
        market_date=calendar_gate.market_date,
        market_status=calendar_gate.market_status,
        should_run=calendar_gate.should_run,
        calendar_verified=calendar_gate.calendar_verified,
        reason=calendar_gate.reason,
        source="TWSE_CALENDAR_PLUS_DGPA",
        calendar_name=calendar_gate.calendar_name,
        calendar_description=calendar_gate.calendar_description,
        emergency_verified=emergency.verified,
        emergency_source=emergency.source,
        emergency_reason=emergency.reason,
        emergency_page_date=emergency.page_date,
        emergency_detail=emergency.detail,
    )


def target_date() -> date:
    if OVERRIDE_DATE:
        return date.fromisoformat(OVERRIDE_DATE)
    return datetime.now(TZ).date()


def main() -> int:
    gate = determine_market_gate(target_date())
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = gate.as_dict()
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(payload, ensure_ascii=False))
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
            "emergency_verified": False,
            "reason": f"FATAL_FAIL_OPEN_WEEKDAY: {type(exc).__name__}: {exc}",
            "source": "TWSE_CALENDAR_PLUS_DGPA",
            "checked_at": datetime.now(TZ).isoformat(),
        }
        OUTPUT_PATH.write_text(json.dumps(fallback, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(fallback, ensure_ascii=False))
        sys.exit(0)
