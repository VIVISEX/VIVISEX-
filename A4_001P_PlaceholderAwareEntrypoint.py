from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
CORE = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
ADAPTER_VERSION = "A4_001P_RECOVERY_V1.1.0"
FAST_BATCH_RETRY_TIMEOUT = max(0.35, float(os.getenv("FAST_BATCH_RETRY_TIMEOUT", "0.95")))
FAST_BATCH_RETRY_WORKERS = max(1, int(os.getenv("FAST_BATCH_RETRY_WORKERS", "1")))


def load_core():
    spec = importlib.util.spec_from_file_location("a4_core_placeholder_aware", CORE)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load A4 core")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def repair_aligned_items(batch: list[Any], items: list[Any], raw: dict[str, Any]) -> list[dict[str, Any]]:
    # Positional repair is safe only if the response is exactly one-for-one AND
    # every nonblank code is already at the expected position. Otherwise fail closed.
    if len(items) != len(batch):
        return [dict(x) if isinstance(x, dict) else {} for x in items]
    for stock, raw_item in zip(batch, items):
        item = raw_item if isinstance(raw_item, dict) else {}
        code = str(item.get("c", "")).strip()
        if code and code != stock.code:
            return [dict(x) if isinstance(x, dict) else {} for x in items]

    response_date = str((raw.get("queryTime") or {}).get("sysDate", "")).strip()
    repaired: list[dict[str, Any]] = []
    for stock, raw_item in zip(batch, items):
        item = dict(raw_item) if isinstance(raw_item, dict) else {}
        if not str(item.get("c", "")).strip():
            item["_qts_original_c"] = item.get("c")
            item["_qts_original_d"] = item.get("d")
            item["c"] = stock.code
            item["_qts_placeholder"] = True
            item["_qts_source_state"] = "NO_QUOTE_DATA"
            item["_qts_requested_code"] = stock.code
            item["_qts_requested_ex_ch"] = stock.ex_ch
            item["_qts_response_date"] = response_date
            if response_date and not str(item.get("d", "")).strip():
                item["d"] = response_date
                item["_qts_date_derived_from"] = "queryTime.sysDate"
        repaired.append(item)
    return repaired


def install(mod: Any) -> Any:
    original_fetch_once = mod.fetch_once
    original_normalize = mod.normalize_item

    def aligned_fetch_once(batch, timeout, stage):
        result = original_fetch_once(batch, timeout, stage)
        if result.ok and isinstance(result.raw, dict):
            result.items = repair_aligned_items(batch, result.items, result.raw)
        return result

    def placeholder_normalize(item, captured_at, phase):
        row = original_normalize(item, captured_at, phase)
        placeholder = bool(item.get("_qts_placeholder", False))
        if not row.get("market_date") and item.get("_qts_response_date"):
            row["market_date"] = str(item.get("_qts_response_date"))
        row["source"] = "TWSE_MIS"
        row["source_state"] = "NO_QUOTE_DATA" if placeholder else "QUOTE_DATA"
        row["signal_eligible"] = not placeholder
        row["date_derived_from_query"] = bool(item.get("_qts_date_derived_from"))
        return row

    def robust_capture_cycle(stocks, cycle_deadline):
        by_code = {s.code: s for s in stocks}
        requested_codes = set(by_code)
        all_results = []
        best_items: dict[str, dict[str, Any]] = {}

        def absorb(results):
            for result in results:
                for item in result.items:
                    code = str(item.get("c", "")).strip()
                    if code in requested_codes:
                        best_items[code] = item

        primary_batches = mod.chunks(stocks, mod.BATCH_SIZE)
        primary = mod.run_parallel(primary_batches, mod.PRIMARY_WORKERS, mod.PRIMARY_TIMEOUT, "primary", cycle_deadline)
        all_results.extend(primary)
        absorb(primary)

        # If a whole primary batch timed out/failed, retry that SAME batch first using
        # a fresh executor/thread/session. This is much faster than immediately exploding
        # an 80-symbol outage into many small requests inside a 5-second cycle budget.
        failed_batches = [r.requested for r in primary if (not r.ok and r.requested)]
        if failed_batches and time.monotonic() < cycle_deadline - mod.CYCLE_GUARD_SECONDS:
            fast = mod.run_parallel(
                failed_batches,
                min(FAST_BATCH_RETRY_WORKERS, len(failed_batches)),
                FAST_BATCH_RETRY_TIMEOUT,
                "retry_failed_batch",
                cycle_deadline,
            )
            all_results.extend(fast)
            absorb(fast)

        missing = requested_codes - set(best_items)
        if missing and time.monotonic() < cycle_deadline - mod.CYCLE_GUARD_SECONDS:
            retry_stocks = [by_code[c] for c in sorted(missing)]
            retry = mod.run_parallel(
                mod.chunks(retry_stocks, mod.RETRY_BATCH_SIZE),
                mod.RETRY_WORKERS,
                mod.RETRY_TIMEOUT,
                "retry_small_batch",
                cycle_deadline,
            )
            all_results.extend(retry)
            absorb(retry)

        missing = requested_codes - set(best_items)
        if missing and len(missing) <= mod.SINGLE_FALLBACK_LIMIT and time.monotonic() < cycle_deadline - mod.CYCLE_GUARD_SECONDS:
            singles = mod.run_parallel(
                [[by_code[c]] for c in sorted(missing)],
                min(mod.RETRY_WORKERS, len(missing)),
                mod.SINGLE_TIMEOUT,
                "single_fallback",
                cycle_deadline,
            )
            all_results.extend(singles)
            absorb(singles)

        missing_final = sorted(requested_codes - set(best_items))
        placeholders = sorted(
            code for code, item in best_items.items()
            if isinstance(item, dict) and item.get("_qts_placeholder")
        )
        diagnostics = {
            "version": "1.4.2",
            "adapter_version": ADAPTER_VERSION,
            "request_attempts": len(all_results),
            "request_errors": sum(1 for r in all_results if not r.ok),
            "primary_requests": len(primary),
            "failed_primary_batches": len(failed_batches),
            "fast_batch_retry_requests": sum(1 for r in all_results if r.stage == "retry_failed_batch"),
            "retry_requests": sum(1 for r in all_results if r.stage == "retry_small_batch"),
            "single_requests": sum(1 for r in all_results if r.stage == "single_fallback"),
            "missing_symbols": missing_final,
            "returned_symbols": len(best_items),
            "requested_symbols": len(requested_codes),
            "placeholder_symbols": placeholders,
            "placeholder_count": len(placeholders),
            "quote_data_symbols": max(0, len(best_items) - len(placeholders)),
            "cycle_elapsed_seconds": None,
        }
        merged = mod.FetchResult(
            stocks,
            [best_items[c] for c in sorted(best_items)],
            None,
            not missing_final,
            None if not missing_final else f"missing symbols: {','.join(missing_final)}",
            0.0,
            "merged_cycle",
        )
        return [merged] + all_results, diagnostics

    mod.fetch_once = aligned_fetch_once
    mod.normalize_item = placeholder_normalize
    mod.capture_cycle = robust_capture_cycle
    mod.VERSION = "1.4.2"
    return mod


def selftest() -> int:
    mod = install(load_core())
    batch = [mod.Stock("TSE", "8105", "tse_8105.tw"), mod.Stock("TSE", "2330", "tse_2330.tw")]
    raw = {
        "msgArray": [
            {"tv": "-", "s": "-", "c": "", "z": "-"},
            {"c": "2330", "d": "20260819", "t": "08:48:00", "ts": "1", "pz": "100"},
        ],
        "queryTime": {"sysDate": "20260819"},
        "rtcode": "0000",
    }
    items = repair_aligned_items(batch, raw["msgArray"], raw)
    placeholder_row = mod.normalize_item(items[0], mod.now_tw(), "PREOPEN")
    real_row = mod.normalize_item(items[1], mod.now_tw(), "PREOPEN")

    # A nonblank code at the wrong position must disable positional repair.
    unsafe_raw = {"msgArray": [{"c": "2330"}, {"c": ""}], "queryTime": {"sysDate": "20260819"}}
    unsafe = repair_aligned_items(batch, unsafe_raw["msgArray"], unsafe_raw)

    # Fault-inject: first primary batch fails completely; same-batch fast retry succeeds.
    stocks = [mod.Stock("TSE", str(1000 + i), f"tse_{1000+i}.tw") for i in range(4)]
    original_run_parallel = mod.run_parallel
    try:
        def fake_run_parallel(batches, workers, timeout, stage, deadline):
            if stage == "primary":
                return [mod.FetchResult(batches[0], [], None, False, "forced timeout", 0.1, stage)]
            if stage == "retry_failed_batch":
                return [mod.FetchResult(batches[0], [{"c": s.code, "d": "20260819"} for s in batches[0]], {"rtcode": "0000"}, True, None, 0.1, stage)]
            return []
        mod.run_parallel = fake_run_parallel
        recovered, diag = mod.capture_cycle(stocks, time.monotonic() + 2.0)
        fast_retry_ok = recovered[0].ok and diag["returned_symbols"] == 4 and diag["fast_batch_retry_requests"] == 1
    finally:
        mod.run_parallel = original_run_parallel

    checks = {
        "placeholder_code_restored": items[0]["c"] == "8105",
        "response_date_preserved": placeholder_row["market_date"] == "20260819",
        "date_derivation_labeled": placeholder_row["date_derived_from_query"] is True,
        "placeholder_not_signal_eligible": placeholder_row["signal_eligible"] is False,
        "placeholder_state_explicit": placeholder_row["source_state"] == "NO_QUOTE_DATA",
        "real_quote_signal_eligible": real_row["signal_eligible"] is True,
        "no_fake_price": placeholder_row.get("raw_pz") is None and placeholder_row.get("last_price") == "-",
        "mismatch_fails_closed": str(unsafe[1].get("c", "")) == "",
        "failed_batch_fast_retry_recovers": fast_retry_ok,
    }
    report = {"component": "A4_001P", "version": ADAPTER_VERSION, "checks": checks, "pass": all(checks.values())}
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 2


def main() -> int:
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--self-test", action="store_true")
    args, _ = p.parse_known_args()
    if args.self_test:
        return selftest()
    mod = install(load_core())
    return mod.main()


if __name__ == "__main__":
    raise SystemExit(main())
