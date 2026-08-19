from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
ROOT = Path(__file__).resolve().parent
ADAPTER = ROOT / "A4_001P_PlaceholderAwareEntrypoint.py"
VERSION = "A4_001W_SUSTAINED_TRANSPORT_V1.0.0"
OUT = Path(os.getenv("A4_SUSTAINED_OUT", "output/sustained_transport"))


def load_adapter():
    spec = importlib.util.spec_from_file_location("a4_placeholder_adapter_for_sustained", ADAPTER)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load placeholder adapter")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def run(cycles: int, interval: float) -> dict[str, Any]:
    adapter = load_adapter()
    core = adapter.install(adapter.load_core())
    stocks = core.load_stocks(core.STOCKS_FILE)
    requested = len(stocks)
    OUT.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    next_target = time.monotonic()

    for idx in range(cycles):
        now_mono = time.monotonic()
        if now_mono < next_target:
            time.sleep(next_target - now_mono)
        started = time.monotonic()
        results, diag = core.capture_cycle(stocks, started + core.CYCLE_BUDGET_SECONDS)
        elapsed = time.monotonic() - started
        merged = results[0]
        returned = len({str(x.get("c", "")) for x in merged.items if isinstance(x, dict) and str(x.get("c", ""))})
        errors = [
            {"stage": r.stage, "error": r.error, "requested": len(r.requested), "elapsed_seconds": r.elapsed_seconds}
            for r in results[1:] if not r.ok
        ]
        row = {
            "cycle": idx,
            "at": datetime.now(TZ).isoformat(timespec="milliseconds"),
            "requested_symbols": requested,
            "returned_symbols": returned,
            "missing_symbols": diag.get("missing_symbols", []),
            "placeholder_symbols": diag.get("placeholder_symbols", []),
            "request_attempts": diag.get("request_attempts"),
            "request_errors": diag.get("request_errors"),
            "elapsed_seconds": elapsed,
            "errors": errors,
            "pass": returned == requested and not diag.get("missing_symbols") and elapsed <= core.CYCLE_BUDGET_SECONDS + 0.25,
        }
        rows.append(row)
        print(json.dumps(row, ensure_ascii=False), flush=True)
        next_target += interval
        if next_target < time.monotonic():
            next_target = time.monotonic()

    successes = sum(1 for r in rows if r["pass"])
    summary = {
        "component": "A4_001W",
        "version": VERSION,
        "checked_at": datetime.now(TZ).isoformat(),
        "config": {
            "cycles": cycles,
            "interval_seconds": interval,
            "batch_size": core.BATCH_SIZE,
            "primary_workers": core.PRIMARY_WORKERS,
            "retry_batch_size": core.RETRY_BATCH_SIZE,
            "retry_workers": core.RETRY_WORKERS,
            "single_fallback_limit": core.SINGLE_FALLBACK_LIMIT,
            "cycle_budget_seconds": core.CYCLE_BUDGET_SECONDS,
        },
        "requested_symbols": requested,
        "successful_cycles": successes,
        "failed_cycles": cycles - successes,
        "success_rate": successes / cycles if cycles else 0.0,
        "max_elapsed_seconds": max((r["elapsed_seconds"] for r in rows), default=0.0),
        "avg_elapsed_seconds": mean([r["elapsed_seconds"] for r in rows]) if rows else 0.0,
        "total_request_errors": sum(int(r.get("request_errors") or 0) for r in rows),
        "failed_cycle_details": [r for r in rows if not r["pass"]],
        "placeholder_union": sorted({c for r in rows for c in (r.get("placeholder_symbols") or [])}),
        "pass": cycles > 0 and successes == cycles,
    }
    (OUT / "cycles.ndjson").write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in rows) + "\n", encoding="utf-8")
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return summary


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--cycles", type=int, default=60)
    p.add_argument("--interval", type=float, default=5.0)
    a = p.parse_args()
    rep = run(max(1, a.cycles), max(0.5, a.interval))
    return 0 if rep["pass"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
