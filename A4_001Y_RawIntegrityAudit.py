from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
import tempfile
from collections import Counter
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_001Y_RAW_AUDIT_V1.0.0"
ROOT = Path(__file__).resolve().parent
SCRAPER = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output/mis"))
STOCKS_FILE = Path(os.getenv("STOCKS_FILE", "stocks.csv"))
POLL_SECONDS = max(1.0, float(os.getenv("POLL_SECONDS", "5")))
MIN_PREOPEN_COVERAGE = float(os.getenv("MIN_PREOPEN_COVERAGE", "0.98"))
MIN_OPEN_COVERAGE = float(os.getenv("MIN_OPEN_COVERAGE", "0.95"))
MIN_SYMBOL_COVERAGE = float(os.getenv("MIN_SYMBOL_COVERAGE", "1.0"))
SOURCE_GAP_CONSECUTIVE_CYCLES = int(os.getenv("SOURCE_GAP_CONSECUTIVE_CYCLES", "3"))
START_TIME = dtime.fromisoformat(os.getenv("START_TIME", "08:30:00"))
PREOPEN_END = dtime.fromisoformat(os.getenv("PREOPEN_END", "09:00:00"))
END_TIME = dtime.fromisoformat(os.getenv("END_TIME", "09:30:00"))


def load_scraper():
    spec = importlib.util.spec_from_file_location("a4_scraper_for_audit", SCRAPER)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load scraper")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def count_stock_file(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        return len({str(r.get("code", "")).strip() for r in reader if str(r.get("code", "")).strip()})


def expected_cycles(start: dtime, end: dtime) -> int:
    a = datetime.combine(datetime.now(TZ).date(), start, TZ)
    b = datetime.combine(datetime.now(TZ).date(), end, TZ)
    return int((b - a).total_seconds() // POLL_SECONDS)


def parse_raw(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    records: list[dict[str, Any]] = []
    errors: list[str] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                if not isinstance(obj, dict):
                    raise ValueError("record not object")
                records.append(obj)
            except Exception as exc:
                errors.append(f"line={lineno}:{type(exc).__name__}:{exc}")
    return records, errors


def choose_best_records(records: list[dict[str, Any]]) -> tuple[dict[int, dict[str, Any]], int]:
    best: dict[int, dict[str, Any]] = {}
    duplicates = 0
    for rec in records:
        try:
            idx = int(rec.get("cycle_index"))
        except Exception:
            continue
        old = best.get(idx)
        if old is None:
            best[idx] = rec
            continue
        duplicates += 1
        old_codes = {str(x.get("c", "")) for x in (old.get("items") or []) if isinstance(x, dict)}
        new_codes = {str(x.get("c", "")) for x in (rec.get("items") or []) if isinstance(x, dict)}
        if len(new_codes) > len(old_codes):
            best[idx] = rec
        elif len(new_codes) == len(old_codes) and str(rec.get("captured_at", "")) >= str(old.get("captured_at", "")):
            best[idx] = rec
    return best, duplicates


def max_false_streak(flags: list[bool]) -> int:
    longest = current = 0
    for ok in flags:
        if ok:
            current = 0
        else:
            current += 1
            longest = max(longest, current)
    return longest


def audit(path: Path, day: str, expected_symbols: int, *, write_rebuilt: bool = True) -> dict[str, Any]:
    if expected_symbols <= 0:
        raise RuntimeError("expected_symbols must be >0")
    records, parse_errors = parse_raw(path)
    best, duplicate_records = choose_best_records(records)
    day_compact = day.replace("-", "")
    pre_expected = expected_cycles(START_TIME, PREOPEN_END)
    open_expected = expected_cycles(PREOPEN_END, END_TIME)
    total_expected = pre_expected + open_expected

    valid_by_idx: dict[int, bool] = {}
    symbol_coverage_by_idx: dict[int, float] = {}
    semantic_ready_by_idx: dict[int, float] = {}
    five_level_by_idx: dict[int, float] = {}
    missing_counter: Counter[str] = Counter()
    stale_rows = 0
    rebuilt_rows: list[dict[str, Any]] = []
    scraper = load_scraper() if write_rebuilt else None

    for idx, rec in sorted(best.items()):
        items = [x for x in (rec.get("items") or []) if isinstance(x, dict)]
        by_code: dict[str, dict[str, Any]] = {}
        for item in items:
            code = str(item.get("c", "")).strip()
            if code:
                by_code[code] = item
        codes = set(by_code)
        fresh_codes = {c for c, x in by_code.items() if str(x.get("d", "")) == day_compact}
        stale_rows += sum(1 for x in by_code.values() if str(x.get("d", "")) != day_compact)
        coverage = len(fresh_codes) / expected_symbols
        symbol_coverage_by_idx[idx] = coverage
        valid_by_idx[idx] = coverage >= MIN_SYMBOL_COVERAGE

        diag = rec.get("diagnostics") or {}
        for code in diag.get("missing_symbols", []) or []:
            missing_counter[str(code)] += 1

        if rec.get("phase") == "PREOPEN":
            semantic_ready = 0
            five = 0
            for code in fresh_codes:
                item = by_code[code]
                if str(item.get("ts", "")) == "1":
                    semantic_ready += 1
                level_fields = ("b", "a", "g", "f")
                if all(len([p for p in str(item.get(k, "")).split("_") if p != ""]) >= 5 for k in level_fields):
                    five += 1
            semantic_ready_by_idx[idx] = semantic_ready / expected_symbols
            five_level_by_idx[idx] = five / expected_symbols

        if write_rebuilt and scraper is not None:
            captured = datetime.fromisoformat(str(rec.get("captured_at")))
            phase = str(rec.get("phase", ""))
            for code, item in by_code.items():
                row = scraper.normalize_item(item, captured, phase)
                row["cycle_index"] = idx
                rebuilt_rows.append(row)

    pre_flags = [valid_by_idx.get(i, False) for i in range(pre_expected)]
    open_flags = [valid_by_idx.get(i, False) for i in range(pre_expected, total_expected)]
    pre_valid = sum(pre_flags)
    open_valid = sum(open_flags)
    pre_coverage = pre_valid / pre_expected if pre_expected else 0.0
    open_coverage = open_valid / open_expected if open_expected else 0.0
    all_flags = pre_flags + open_flags
    max_bad_streak = max_false_streak(all_flags)
    min_symbol_coverage = min(symbol_coverage_by_idx.values()) if symbol_coverage_by_idx else 0.0

    gates = {
        "raw_parse_clean": not parse_errors,
        "preopen_coverage_ok": pre_coverage >= MIN_PREOPEN_COVERAGE,
        "open_coverage_ok": open_coverage >= MIN_OPEN_COVERAGE,
        "symbol_coverage_ok": min_symbol_coverage >= MIN_SYMBOL_COVERAGE,
        "source_gap_not_persistent": max_bad_streak < SOURCE_GAP_CONSECUTIVE_CYCLES,
        "fresh_market_date_only": stale_rows == 0,
    }

    report = {
        "component": "A4_001Y",
        "version": VERSION,
        "checked_at": datetime.now(TZ).isoformat(),
        "date": day,
        "raw_file": str(path),
        "expected_symbols": expected_symbols,
        "raw_records": len(records),
        "unique_cycles": len(best),
        "duplicate_cycle_records_resolved": duplicate_records,
        "parse_errors": parse_errors,
        "expected_preopen_cycles": pre_expected,
        "valid_preopen_cycles": pre_valid,
        "preopen_cycle_coverage": pre_coverage,
        "expected_open_cycles": open_expected,
        "valid_open_cycles": open_valid,
        "open_cycle_coverage": open_coverage,
        "min_symbol_coverage": min_symbol_coverage,
        "max_consecutive_bad_cycles": max_bad_streak,
        "stale_rows": stale_rows,
        "most_frequent_missing_symbols": missing_counter.most_common(20),
        "preopen_semantic_ready_min": min(semantic_ready_by_idx.values()) if semantic_ready_by_idx else 0.0,
        "preopen_five_level_min": min(five_level_by_idx.values()) if five_level_by_idx else 0.0,
        "gates": gates,
        "pass": all(gates.values()),
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUTPUT_DIR / f"integrity_{day}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if write_rebuilt and rebuilt_rows:
        df = pd.DataFrame(rebuilt_rows).sort_values(["cycle_index", "code"]).drop_duplicates(["cycle_index", "code"], keep="last")
        df.to_csv(OUTPUT_DIR / f"normalized_rebuilt_{day}.csv", index=False, encoding="utf-8-sig")
        df.to_parquet(OUTPUT_DIR / f"normalized_rebuilt_{day}.parquet", index=False)
    print(json.dumps(report, ensure_ascii=False))
    return report


def selftest() -> int:
    global OUTPUT_DIR, START_TIME, PREOPEN_END, END_TIME, POLL_SECONDS, MIN_PREOPEN_COVERAGE, MIN_OPEN_COVERAGE, MIN_SYMBOL_COVERAGE, SOURCE_GAP_CONSECUTIVE_CYCLES
    original = (OUTPUT_DIR, START_TIME, PREOPEN_END, END_TIME, POLL_SECONDS, MIN_PREOPEN_COVERAGE, MIN_OPEN_COVERAGE, MIN_SYMBOL_COVERAGE, SOURCE_GAP_CONSECUTIVE_CYCLES)
    try:
        with tempfile.TemporaryDirectory() as td:
            OUTPUT_DIR = Path(td) / "out"
            POLL_SECONDS = 5.0
            START_TIME = dtime(8, 30, 0)
            PREOPEN_END = dtime(8, 30, 10)
            END_TIME = dtime(8, 30, 20)
            MIN_PREOPEN_COVERAGE = 1.0
            MIN_OPEN_COVERAGE = 1.0
            MIN_SYMBOL_COVERAGE = 1.0
            SOURCE_GAP_CONSECUTIVE_CYCLES = 2
            raw = Path(td) / "raw.ndjson"
            rows = []
            for idx in range(4):
                phase = "PREOPEN" if idx < 2 else "OPEN_VALIDATION"
                items = []
                for code in ("2330", "2317"):
                    items.append({"c": code, "d": "20260819", "t": "08:30:00", "ts": "1", "b": "1_1_1_1_1_", "a": "1_1_1_1_1_", "g": "1_1_1_1_1_", "f": "1_1_1_1_1_"})
                rows.append({"cycle_index": idx, "captured_at": f"2026-08-19T08:30:{idx*5:02d}+08:00", "phase": phase, "diagnostics": {"requested_symbols": 2, "missing_symbols": []}, "items": items})
            # duplicate/restart record with worse coverage; audit must keep the better record
            rows.append({"cycle_index": 1, "captured_at": "2026-08-19T08:30:06+08:00", "phase": "PREOPEN", "diagnostics": {"requested_symbols": 2, "missing_symbols": ["2317"]}, "items": [rows[1]["items"][0]]})
            raw.write_text("\n".join(json.dumps(x) for x in rows) + "\n", encoding="utf-8")
            rep = audit(raw, "2026-08-19", 2, write_rebuilt=False)
            checks = {
                "pass": rep["pass"],
                "dedup": rep["duplicate_cycle_records_resolved"] == 1,
                "all_cycles": rep["unique_cycles"] == 4,
                "full_symbol_coverage": rep["min_symbol_coverage"] == 1.0,
            }
            result = {"component": "A4_001Y", "version": VERSION, "mode": "selftest", "checks": checks, "pass": all(checks.values())}
            print(json.dumps(result, ensure_ascii=False))
            return 0 if result["pass"] else 2
    finally:
        OUTPUT_DIR, START_TIME, PREOPEN_END, END_TIME, POLL_SECONDS, MIN_PREOPEN_COVERAGE, MIN_OPEN_COVERAGE, MIN_SYMBOL_COVERAGE, SOURCE_GAP_CONSECUTIVE_CYCLES = original


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--date")
    p.add_argument("--raw")
    p.add_argument("--expected-symbols", type=int)
    args = p.parse_args()
    if args.self_test:
        return selftest()
    day = args.date or datetime.now(TZ).strftime("%Y-%m-%d")
    raw = Path(args.raw) if args.raw else OUTPUT_DIR / f"raw_{day}.ndjson"
    expected = args.expected_symbols or count_stock_file(STOCKS_FILE)
    if not raw.exists():
        raise SystemExit(f"raw file missing: {raw}")
    rep = audit(raw, day, expected, write_rebuilt=True)
    return 0 if rep["pass"] else 3


if __name__ == "__main__":
    raise SystemExit(main())
