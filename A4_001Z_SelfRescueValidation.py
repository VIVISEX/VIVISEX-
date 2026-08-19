from __future__ import annotations

import importlib.util
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_001Z_SELF_RESCUE_VALIDATION_V1.0.0"
ROOT = Path(__file__).resolve().parent
SCRAPER = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
OUT = ROOT / "status" / "a4" / "a4_001z_self_rescue_latest.json"


def load_scraper():
    spec = importlib.util.spec_from_file_location("a4_scraper_validation", SCRAPER)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load scraper")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def item(code: str, day: str = "20260819") -> dict[str, Any]:
    return {
        "ex": "tse", "c": code, "n": code, "d": day, "t": "08:45:00", "ts": "1",
        "z": "-", "pz": "100", "ps": "10", "v": "100", "y": "99",
        "b": "99_98_97_96_95_", "g": "1_2_3_4_5_",
        "a": "100_101_102_103_104_", "f": "1_2_3_4_5_",
    }


def fake_result(mod, batch, items, ok=True, stage="primary"):
    return mod.FetchResult(batch, items, {"rtcode": "0000"}, ok, None if ok else "forced", 0.01, stage)


def recovery_case_retry_batch(mod) -> bool:
    stocks = [mod.Stock("TSE", c, f"tse_{c}.tw") for c in ("1001", "1002", "1003")]
    original = mod.run_parallel
    try:
        def fake(batches, workers, timeout, stage, deadline):
            flat = [s for b in batches for s in b]
            if stage == "primary":
                return [fake_result(mod, flat, [item("1001"), item("1002")], True, stage)]
            if stage == "retry_small_batch":
                return [fake_result(mod, flat, [item("1003")], True, stage)]
            return []
        mod.run_parallel = fake
        results, diag = mod.capture_cycle(stocks, time.monotonic() + 2)
        codes = {str(x.get("c")) for x in results[0].items}
        return codes == {"1001", "1002", "1003"} and diag["missing_symbols"] == [] and diag["retry_requests"] >= 1
    finally:
        mod.run_parallel = original


def recovery_case_single_fallback(mod) -> bool:
    stocks = [mod.Stock("TSE", c, f"tse_{c}.tw") for c in ("1001", "1002", "1003")]
    original = mod.run_parallel
    try:
        def fake(batches, workers, timeout, stage, deadline):
            flat = [s for b in batches for s in b]
            if stage == "primary":
                return [fake_result(mod, flat, [item("1001"), item("1002")], True, stage)]
            if stage == "retry_small_batch":
                return [fake_result(mod, flat, [], True, stage)]
            if stage == "single_fallback":
                return [fake_result(mod, b, [item(b[0].code)], True, stage) for b in batches]
            return []
        mod.run_parallel = fake
        results, diag = mod.capture_cycle(stocks, time.monotonic() + 2)
        return results[0].ok and diag["missing_symbols"] == [] and diag["single_requests"] >= 1
    finally:
        mod.run_parallel = original


def recovery_case_fail_closed(mod) -> bool:
    stocks = [mod.Stock("TSE", c, f"tse_{c}.tw") for c in ("1001", "1002", "1003")]
    original = mod.run_parallel
    try:
        def fake(batches, workers, timeout, stage, deadline):
            flat = [s for b in batches for s in b]
            if stage == "primary":
                return [fake_result(mod, flat, [item("1001"), item("1002")], True, stage)]
            return [fake_result(mod, b, [], False, stage) for b in batches]
        mod.run_parallel = fake
        results, diag = mod.capture_cycle(stocks, time.monotonic() + 2)
        return (not results[0].ok) and diag["missing_symbols"] == ["1003"]
    finally:
        mod.run_parallel = original


def deterministic_stress(mod, loops: int = 20000) -> bool:
    captured = datetime(2026, 8, 19, 8, 45, tzinfo=TZ)
    fingerprints = set()
    sample = item("2330")
    for _ in range(loops):
        row = mod.normalize_item(sample, captured, "PREOPEN")
        fingerprints.add((row["code"], row["raw_pz"], row["raw_ps"], row["bid_price_5"], row["ask_volume_5"], row["simulated_flag"]))
    return len(fingerprints) == 1


def run() -> dict[str, Any]:
    mod = load_scraper()
    checks = {
        "retry_small_batch_recovers_missing": recovery_case_retry_batch(mod),
        "single_fallback_recovers_missing": recovery_case_single_fallback(mod),
        "unrecoverable_missing_fails_closed": recovery_case_fail_closed(mod),
        "deterministic_20000_loops": deterministic_stress(mod),
        "cycle_budget_lt_poll": mod.CYCLE_BUDGET_SECONDS < mod.POLL_SECONDS,
        "source_gap_bounded": mod.SOURCE_GAP_CONSECUTIVE_CYCLES >= 2,
    }
    return {
        "component": "A4_001Z",
        "version": VERSION,
        "checked_at": datetime.now(TZ).isoformat(),
        "scraper_version": getattr(mod, "VERSION", "unknown"),
        "checks": checks,
        "pass": all(checks.values()),
    }


def main() -> int:
    try:
        report = run()
    except Exception as exc:
        report = {"component": "A4_001Z", "version": VERSION, "checked_at": datetime.now(TZ).isoformat(), "pass": False, "error": f"{type(exc).__name__}: {exc}"}
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report.get("pass") else 2


if __name__ == "__main__":
    raise SystemExit(main())
