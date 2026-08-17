from __future__ import annotations

import importlib.util
import json
import math
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_001R_DURABILITY_V1.0.1"
ROOT = Path(__file__).resolve().parent
SCRAPER = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
OUT = ROOT / "status" / "a4" / "a4_001r_durability_latest.json"
TWSE_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"


def write_report(report: dict[str, Any]) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")


def load_scraper():
    spec = importlib.util.spec_from_file_location("a4_prod", SCRAPER)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load production scraper")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def official_universe(limit: int = 200) -> list[str]:
    r = requests.get(TWSE_ALL, timeout=15)
    r.raise_for_status()
    rows = r.json()
    if not isinstance(rows, list):
        raise RuntimeError("TWSE STOCK_DAY_ALL not list")
    codes: list[str] = []
    for row in rows:
        code = str(row.get("Code") or row.get("證券代號") or "").strip()
        if len(code) == 4 and code.isdigit():
            codes.append(code)
        if len(codes) >= limit:
            break
    if len(codes) < limit:
        raise RuntimeError(f"official universe too small: {len(codes)}")
    return codes


def make_stocks(mod, codes: list[str]):
    return [mod.Stock(market="TSE", code=c, ex_ch=f"tse_{c}.tw") for c in codes]


def one_transport_pass(mod, stocks) -> dict[str, Any]:
    session = mod.make_session(warmup=True)
    started = time.perf_counter()
    returned: set[str] = set()
    requests_count = 0
    fallback_count = 0
    errors: list[str] = []
    try:
        for batch in mod.chunks(stocks, mod.BATCH_SIZE):
            requests_count += 1
            try:
                raw = mod.fetch_batch(session, batch)
                if raw.get("_qts_transport", {}).get("mode") != "batch":
                    fallback_count += 1
                for item in raw.get("msgArray", []) or []:
                    code = str(item.get("c", "")).strip()
                    if code:
                        returned.add(code)
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {exc}")
    finally:
        session.close()
    elapsed = time.perf_counter() - started
    requested = {s.code for s in stocks}
    return {
        "symbols": len(stocks),
        "elapsed_seconds": round(elapsed, 4),
        "requests": requests_count,
        "returned_symbols": len(returned & requested),
        "coverage": len(returned & requested) / len(requested) if requested else 0.0,
        "fallback_batches": fallback_count,
        "errors": errors,
        "pass": not errors and requested.issubset(returned),
    }


def repeated_transport(mod, stocks, repeats: int) -> dict[str, Any]:
    runs = [one_transport_pass(mod, stocks) for _ in range(repeats)]
    elapsed = [r["elapsed_seconds"] for r in runs]
    return {
        "symbols": len(stocks),
        "repeats": repeats,
        "runs": runs,
        "successes": sum(1 for r in runs if r["pass"]),
        "min_seconds": min(elapsed),
        "median_seconds": round(statistics.median(elapsed), 4),
        "max_seconds": max(elapsed),
        "p95_seconds": round(sorted(elapsed)[max(0, math.ceil(0.95 * len(elapsed)) - 1)], 4),
        "within_5s_all": max(elapsed) <= 5.0,
        "pass": all(r["pass"] for r in runs),
    }


def deterministic_unit_stress(mod, loops: int = 10000) -> dict[str, Any]:
    item = {
        "ex": "tse", "c": "2330", "n": "TSMC", "d": "20260817", "t": "08:55:00",
        "ts": "1", "z": "-", "pz": "2410", "ps": "123", "v": "1000", "y": "2395",
        "b": "2405_2400_2395_2390_2385_", "g": "10_20_30_40_50_",
        "a": "2410_2415_2420_2425_2430_", "f": "11_21_31_41_51_",
    }
    captured = datetime(2026, 8, 17, 8, 55, tzinfo=TZ)
    started = time.perf_counter()
    fingerprints = set()
    for _ in range(loops):
        row = mod.normalize_item(item, captured, "PREOPEN")
        stable = (
            row["code"], row["market_date"], row["market_time"], row["raw_pz"], row["raw_ps"],
            row["bid_price_1"], row["ask_price_1"], row["bid_volume_5"], row["ask_volume_5"],
        )
        fingerprints.add(stable)
    elapsed = time.perf_counter() - started
    return {
        "loops": loops,
        "elapsed_seconds": round(elapsed, 4),
        "unique_outputs": len(fingerprints),
        "pass": len(fingerprints) == 1,
    }


def run() -> dict[str, Any]:
    mod = load_scraper()
    codes = official_universe(200)
    sizes_repeats = [(5, 5), (20, 5), (50, 5), (100, 3), (200, 3)]
    scale: dict[str, Any] = {}
    for size, repeats in sizes_repeats:
        scale[str(size)] = repeated_transport(mod, make_stocks(mod, codes[:size]), repeats)
    unit = deterministic_unit_stress(mod)
    tier1_ok = scale["5"]["pass"] and scale["20"]["pass"] and scale["20"]["within_5s_all"]
    large_polling_ok = all(scale[str(n)]["pass"] and scale[str(n)]["within_5s_all"] for n in (50, 100, 200))
    return {
        "component": "A4_001R",
        "version": VERSION,
        "checked_at": datetime.now(TZ).isoformat(),
        "official_universe_source": TWSE_ALL,
        "batch_size": mod.BATCH_SIZE,
        "poll_seconds": mod.POLL_SECONDS,
        "unit_stress": unit,
        "scale": scale,
        "tier1_20_transport_candidate": tier1_ok,
        "large_universe_5s_twse_polling_candidate": large_polling_ok,
        "architecture_decision": (
            "TWSE_MIS_OK_FOR_TIER1_UP_TO_20_AND_LARGE_UNIVERSE_OK"
            if tier1_ok and large_polling_ok else
            "TWSE_MIS_OK_FOR_TIER1_UP_TO_20_BUT_LARGE_UNIVERSE_REQUIRES_STREAMING_OR_LOWER_FREQUENCY"
            if tier1_ok else
            "TWSE_MIS_NOT_READY_FOR_TIER1_PRODUCTION"
        ),
        "pass": bool(unit["pass"] and tier1_ok),
    }


def main() -> int:
    try:
        report = run()
    except Exception as exc:
        report = {
            "component": "A4_001R",
            "version": VERSION,
            "checked_at": datetime.now(TZ).isoformat(),
            "pass": False,
            "failure_type": "AUDIT_INFRA_OR_CODE",
            "error": f"{type(exc).__name__}: {exc}",
        }
    write_report(report)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report.get("pass") else 2


if __name__ == "__main__":
    raise SystemExit(main())
