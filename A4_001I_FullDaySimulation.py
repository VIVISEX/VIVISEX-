from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
PROD_PATH = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "a4_mis_2026-08-07_5symbols.json"
OUT_ROOT = ROOT / "output" / "full_day_sim"
TZ = ZoneInfo("Asia/Taipei")
SIM_DAY = datetime(2026, 8, 17, 8, 15, 0, tzinfo=TZ)


def load_prod():
    spec = importlib.util.spec_from_file_location("a4_prod_fullday", PROD_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load production scraper")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


class FakeClock:
    def __init__(self, start: datetime):
        self.current = start
        self.mono = 0.0

    def now(self) -> datetime:
        return self.current

    def monotonic(self) -> float:
        return self.mono

    def sleep(self, seconds: float) -> None:
        seconds = max(0.0, float(seconds))
        self.current += timedelta(seconds=seconds)
        self.mono += seconds


class DummySession:
    def close(self) -> None:
        return None


def fixture_items() -> list[dict[str, Any]]:
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    items = payload.get("msgArray") or []
    require(len(items) == 5, "fixture must contain five symbols")
    return items


def fresh_raw(items: list[dict[str, Any]], captured: datetime, drop_code: str | None = None, stale_code: str | None = None) -> dict[str, Any]:
    out: list[dict[str, Any]] = []
    for source in items:
        item = copy.deepcopy(source)
        code = str(item.get("c", ""))
        if drop_code and code == drop_code:
            continue
        item["d"] = "20260816" if stale_code and code == stale_code else captured.strftime("%Y%m%d")
        item["t"] = captured.strftime("%H:%M:%S")
        item["%"] = captured.strftime("%H:%M:%S")
        out.append(item)
    return {
        "rtcode": "0000",
        "rtmessage": "OK",
        "msgArray": out,
        "queryTime": {"sysDate": captured.strftime("%Y%m%d"), "sysTime": captured.strftime("%H:%M:%S")},
        "_qts_transport": {"mode": "batch", "parts": 1},
    }


def run_scenario(name: str, start: datetime, mode: str) -> dict[str, Any]:
    prod = load_prod()
    items = fixture_items()
    clock = FakeClock(start)
    scenario_dir = OUT_ROOT / name
    if scenario_dir.exists():
        shutil.rmtree(scenario_dir)
    scenario_dir.mkdir(parents=True, exist_ok=True)

    originals = {
        "OUTPUT_DIR": prod.OUTPUT_DIR,
        "now_tw": prod.now_tw,
        "make_session": prod.make_session,
        "fetch_batch": prod.fetch_batch,
        "sleep": prod.time.sleep,
        "monotonic": prod.time.monotonic,
    }
    prod.OUTPUT_DIR = scenario_dir
    prod.now_tw = clock.now
    prod.make_session = lambda warmup=True: DummySession()
    prod.time.sleep = clock.sleep
    prod.time.monotonic = clock.monotonic

    request_no = 0

    def fake_fetch(session, batch):
        nonlocal request_no
        request_no += 1
        captured = clock.now()
        if mode == "transient_errors" and request_no in {101, 401}:
            raise RuntimeError("synthetic transient SOURCE_GAP")
        if mode == "persistent_gap" and request_no in {101, 102, 103}:
            raise RuntimeError("synthetic persistent SOURCE_GAP")
        if mode == "one_missing_symbol" and request_no == 201:
            return fresh_raw(items, captured, drop_code="2382")
        if mode == "one_stale_symbol" and request_no == 201:
            return fresh_raw(items, captured, stale_code="2382")
        return fresh_raw(items, captured)

    prod.fetch_batch = fake_fetch
    stocks = prod.load_stocks(ROOT / "stocks.csv")
    try:
        rc = prod.run_live(stocks)
    finally:
        prod.OUTPUT_DIR = originals["OUTPUT_DIR"]
        prod.now_tw = originals["now_tw"]
        prod.make_session = originals["make_session"]
        prod.fetch_batch = originals["fetch_batch"]
        prod.time.sleep = originals["sleep"]
        prod.time.monotonic = originals["monotonic"]

    report_path = scenario_dir / f"report_{start.strftime('%Y-%m-%d')}.json"
    require(report_path.exists(), f"{name}: report missing")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    return {
        "name": name,
        "mode": mode,
        "return_code": rc,
        "simulated_start": start.isoformat(),
        "simulated_end": clock.now().isoformat(),
        "report": report,
        "raw_exists": (scenario_dir / f"raw_{start.strftime('%Y-%m-%d')}.ndjson").exists(),
        "csv_exists": (scenario_dir / f"normalized_{start.strftime('%Y-%m-%d')}.csv").exists(),
        "parquet_exists": (scenario_dir / f"normalized_{start.strftime('%Y-%m-%d')}.parquet").exists(),
        "heartbeat_exists": (scenario_dir / f"heartbeat_{start.strftime('%Y-%m-%d')}.json").exists(),
    }


def validate() -> dict[str, Any]:
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    scenarios: list[dict[str, Any]] = []

    normal = run_scenario("normal_full_session", SIM_DAY, "normal")
    r = normal["report"]
    require(normal["return_code"] == 0 and r.get("pass") is True, "normal session must pass")
    require(r.get("expected_preopen_cycles") == 360, "normal preopen expected cycles must be 360")
    require(r.get("actual_preopen_valid_cycles") == 360, "normal preopen actual cycles must be 360")
    require(r.get("expected_open_validation_cycles") == 360, "normal open expected cycles must be 360")
    require(r.get("actual_open_validation_valid_cycles") == 360, "normal open actual cycles must be 360")
    require(r.get("preopen_coverage") == 1.0 and r.get("open_validation_coverage") == 1.0, "normal coverage must be 100%")
    require(r.get("max_gap_seconds") == 5.0, "normal max gap must be 5 seconds")
    require(r.get("normalized_rows") == 3600, "normal normalized rows must be 3600")
    require(all(normal[k] for k in ("raw_exists", "csv_exists", "parquet_exists", "heartbeat_exists")), "normal output contract incomplete")
    scenarios.append({"name": normal["name"], "pass": True, "detail": normal})

    transient = run_scenario("transient_two_errors", SIM_DAY, "transient_errors")
    r = transient["report"]
    require(transient["return_code"] == 0 and r.get("pass") is True, "two isolated transient errors should remain within production tolerance")
    require(r.get("error_count") == 2, "transient test must inject exactly two errors")
    require(r.get("max_consecutive_failed_cycles") == 1, "transient errors must not form persistent SOURCE_GAP")
    require(r.get("request_error_rate", 1) <= r.get("allowed_request_error_rate", 0), "transient error rate should be accepted")
    scenarios.append({"name": transient["name"], "pass": True, "detail": transient})

    persistent = run_scenario("persistent_three_cycle_gap", SIM_DAY, "persistent_gap")
    r = persistent["report"]
    require(persistent["return_code"] != 0 and r.get("pass") is False, "persistent gap must fail")
    require(r.get("max_consecutive_failed_cycles", 0) >= 3, "persistent gap threshold not reached")
    require("SOURCE_GAP_NOT_PERSISTENT" in (r.get("failure_reasons") or []), "persistent gap failure gate missing")
    scenarios.append({"name": persistent["name"], "pass": True, "detail": persistent})

    missing = run_scenario("single_cycle_missing_symbol", SIM_DAY, "one_missing_symbol")
    r = missing["report"]
    require(missing["return_code"] != 0 and r.get("pass") is False, "missing symbol must fail strict symbol coverage")
    require("SYMBOL_COVERAGE_OK" in (r.get("failure_reasons") or []), "missing-symbol gate did not fire")
    scenarios.append({"name": missing["name"], "pass": True, "detail": missing})

    stale = run_scenario("single_cycle_stale_market_date", SIM_DAY, "one_stale_symbol")
    r = stale["report"]
    require(stale["return_code"] != 0 and r.get("pass") is False, "stale market date must fail")
    require("FRESH_MARKET_DATE_ONLY" in (r.get("failure_reasons") or []), "stale-date gate did not fire")
    scenarios.append({"name": stale["name"], "pass": True, "detail": stale})

    delayed_start = run_scenario("start_after_startup_deadline", datetime(2026, 8, 17, 8, 29, 50, tzinfo=TZ), "normal")
    r = delayed_start["report"]
    require(delayed_start["return_code"] != 0 and r.get("pass") is False, "08:29:50 start must fail startup deadline")
    require("STARTUP_BEFORE_082945" in (r.get("failure_reasons") or []), "startup deadline gate did not fire")
    scenarios.append({"name": delayed_start["name"], "pass": True, "detail": delayed_start})

    after_end = run_scenario("start_after_end", datetime(2026, 8, 17, 9, 31, 0, tzinfo=TZ), "normal")
    r = after_end["report"]
    require(after_end["return_code"] == 3 and r.get("reason") == "started_after_end_time", "post-09:30 run must be rejected")
    scenarios.append({"name": after_end["name"], "pass": True, "detail": after_end})

    report = {
        "component": "A4_001I",
        "purpose": "Accelerated integration test of the real production run_live loop from 08:15 through 09:30 with DQ fault injection",
        "pass": True,
        "scenario_count": len(scenarios),
        "scenarios": scenarios,
        "scope_note": "This executes the production run_live function with an accelerated deterministic clock and real production DQ/output logic. Network calls are replaced with five-symbol fixture responses. Real TWSE network reachability is validated separately by A4_001T.",
    }
    (OUT_ROOT / "full_day_sim_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return report


if __name__ == "__main__":
    try:
        validate()
    except Exception as exc:
        OUT_ROOT.mkdir(parents=True, exist_ok=True)
        fail = {"component": "A4_001I", "pass": False, "error": f"{type(exc).__name__}: {exc}"}
        (OUT_ROOT / "full_day_sim_report.json").write_text(json.dumps(fail, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(fail, ensure_ascii=False))
        raise
