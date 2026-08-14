from __future__ import annotations

import importlib.util
import json
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
GATE_PATH = ROOT / "A4_001G_TradingDayGate.py"
OUTPUT_DIR = ROOT / "output" / "emergency_gate_test"
TZ = ZoneInfo("Asia/Taipei")


def load_gate_module():
    spec = importlib.util.spec_from_file_location("a4_market_gate", GATE_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load gate module: {GATE_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    gate = load_gate_module()
    started = datetime.now(TZ)
    results: list[dict[str, Any]] = []

    def run(name: str, fn: Callable[[], dict[str, Any] | None]) -> None:
        t0 = time.perf_counter()
        try:
            detail = fn() or {}
            results.append({
                "name": name,
                "pass": True,
                "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                "detail": detail,
            })
        except Exception as exc:
            results.append({
                "name": name,
                "pass": False,
                "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                "error": f"{type(exc).__name__}: {exc}",
            })

    def weekend() -> dict[str, Any]:
        observed = gate.classify_from_calendar(date(2026, 8, 15), [])
        require(observed.market_status == "MARKET_CLOSED", observed.market_status)
        require(observed.should_run is False, "weekend should not run")
        require(observed.reason == "WEEKEND", observed.reason)
        return observed.as_dict()

    def normal_weekday() -> dict[str, Any]:
        observed = gate.classify_from_calendar(date(2026, 8, 17), [])
        require(observed.market_status == "MARKET_OPEN", observed.market_status)
        require(observed.should_run is True, "normal weekday should run")
        return observed.as_dict()

    def full_day_closure() -> dict[str, Any]:
        raw = "<html><body><h1>115年 8月 17日 天然災害停止上班及上課情形</h1><table><tr><td>臺北市</td><td>今天停止上班、停止上課。</td></tr></table></body></html>"
        observed = gate.classify_emergency_html(date(2026, 8, 17), raw)
        require(observed.verified is True, observed.reason)
        require(observed.market_closed is True, observed.reason)
        require(observed.reason == "TAIPEI_FULL_OR_MORNING_WORK_SUSPENSION", observed.reason)
        return observed.__dict__

    def morning_closure() -> dict[str, Any]:
        raw = "<html><body><h1>115年 8月 17日 天然災害停止上班及上課情形</h1><div>臺北市 上午停止上班、停止上課。</div></body></html>"
        observed = gate.classify_emergency_html(date(2026, 8, 17), raw)
        require(observed.market_closed is True, observed.reason)
        return observed.__dict__

    def afternoon_only() -> dict[str, Any]:
        raw = "<html><body><h1>115年 8月 17日 天然災害停止上班及上課情形</h1><div>臺北市 下午停止上班、停止上課。</div></body></html>"
        observed = gate.classify_emergency_html(date(2026, 8, 17), raw)
        require(observed.verified is True, observed.reason)
        require(observed.market_closed is False, observed.reason)
        require(observed.reason == "TAIPEI_AFTERNOON_ONLY_CLOSURE_MARKET_OPEN", observed.reason)
        return observed.__dict__

    def partial_district() -> dict[str, Any]:
        raw = "<html><body><h1>115年 8月 17日 天然災害停止上班及上課情形</h1><div>臺北市 北投區停止上班、停止上課，其他地區照常上班。</div></body></html>"
        observed = gate.classify_emergency_html(date(2026, 8, 17), raw)
        require(observed.verified is True, observed.reason)
        require(observed.market_closed is False, observed.reason)
        return observed.__dict__

    def stale_notice() -> dict[str, Any]:
        raw = "<html><body><h1>115年 8月 16日 天然災害停止上班及上課情形</h1><div>臺北市 今天停止上班、停止上課。</div></body></html>"
        observed = gate.classify_emergency_html(date(2026, 8, 17), raw)
        require(observed.verified is True, observed.reason)
        require(observed.market_closed is False, observed.reason)
        require(observed.reason == "NO_CURRENT_DAY_EMERGENCY_NOTICE", observed.reason)
        return observed.__dict__

    def no_message() -> dict[str, Any]:
        raw = "<html><body><h1>115年 8月 17日 天然災害停止上班及上課情形</h1><div>無停班停課訊息。</div></body></html>"
        observed = gate.classify_emergency_html(date(2026, 8, 17), raw)
        require(observed.verified is True, observed.reason)
        require(observed.market_closed is False, observed.reason)
        return observed.__dict__

    def live_official_probe() -> dict[str, Any]:
        today = datetime.now(TZ).date()
        observed = gate.determine_market_gate(today)
        require(observed.calendar_verified is True, f"TWSE calendar unverified: {observed.reason}")
        if observed.should_run:
            require(observed.emergency_verified is True, f"DGPA emergency check unverified: {observed.emergency_reason}")
        require(observed.market_status in {"MARKET_OPEN", "MARKET_CLOSED", "MARKET_CLOSED_EMERGENCY"}, observed.market_status)
        return observed.as_dict()

    run("weekend_gate", weekend)
    run("normal_weekday_gate", normal_weekday)
    run("synthetic_full_day_emergency_closure", full_day_closure)
    run("synthetic_morning_emergency_closure", morning_closure)
    run("synthetic_afternoon_only_market_stays_open", afternoon_only)
    run("synthetic_partial_district_market_stays_open", partial_district)
    run("synthetic_stale_notice_ignored", stale_notice)
    run("synthetic_no_suspension_notice", no_message)
    run("live_twse_calendar_plus_dgpa_probe", live_official_probe)

    passed = all(item.get("pass") is True for item in results)
    report = {
        "component": "A4_001H",
        "purpose": "Trading-day + emergency market-closure production gate validation",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(TZ).isoformat(),
        "pass": passed,
        "tests": results,
        "scope_note": "Synthetic cases validate TWSE disaster-market rules implemented by A4. Live probe validates GitHub Runner connectivity to the TWSE official holiday calendar and DGPA official daily suspension page. This does not replace the next real 08:30-09:00 capture acceptance.",
    }
    path = OUTPUT_DIR / "emergency_gate_test_report.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
