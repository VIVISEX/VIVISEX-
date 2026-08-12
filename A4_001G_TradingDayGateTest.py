from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import A4_001G_TradingDayGate as gate

OUTPUT = Path("output/readiness/market_gate_test.json")

FIXTURE = [
    {"Name": "農曆春節前最後交易日", "Date": "1150211", "Weekday": "三", "Description": "農曆春節前最後交易。"},
    {"Name": "市場無交易，僅辦理結算交割作業", "Date": "1150212", "Weekday": "四", "Description": ""},
    {"Name": "農曆春節後開始交易日", "Date": "1150223", "Weekday": "一", "Description": "農曆春節後開始交易。"},
    {"Name": "中秋節", "Date": "1150925", "Weekday": "五", "Description": "依規定放假1日。"},
]


def check(day: date, expected_status: str, expected_run: bool, reason: str) -> dict:
    result = gate.classify_from_calendar(day, FIXTURE)
    assert result.market_status == expected_status, (day, result)
    assert result.should_run is expected_run, (day, result)
    return {
        "date": day.isoformat(),
        "market_status": result.market_status,
        "should_run": result.should_run,
        "reason": reason,
    }


def main() -> int:
    cases = [
        check(date(2026, 8, 13), "MARKET_OPEN", True, "normal_weekday"),
        check(date(2026, 9, 25), "MARKET_CLOSED", False, "official_holiday"),
        check(date(2026, 2, 11), "MARKET_OPEN", True, "last_trading_day_before_lunar_new_year"),
        check(date(2026, 2, 12), "MARKET_CLOSED", False, "no_trading_settlement_only"),
        check(date(2026, 2, 23), "MARKET_OPEN", True, "first_trading_day_after_lunar_new_year"),
        check(date(2026, 8, 15), "MARKET_CLOSED", False, "weekend"),
    ]

    live = gate.fetch_calendar()
    live_closed = gate.classify_from_calendar(date(2026, 9, 25), live)
    live_open = gate.classify_from_calendar(date(2026, 2, 11), live)
    assert live_closed.market_status == "MARKET_CLOSED" and not live_closed.should_run
    assert live_open.market_status == "MARKET_OPEN" and live_open.should_run

    report = {
        "component": "A4_001G",
        "pass": True,
        "cases": cases,
        "live_official_api": {
            "rows": len(live),
            "2026-09-25": live_closed.as_dict(),
            "2026-02-11": live_open.as_dict(),
        },
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
