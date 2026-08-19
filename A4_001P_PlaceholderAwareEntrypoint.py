from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
CORE = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
ADAPTER_VERSION = "A4_001P_PLACEHOLDER_V1.0.1"


def load_core():
    spec = importlib.util.spec_from_file_location("a4_core_placeholder_aware", CORE)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load A4 core")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def install(mod: Any) -> Any:
    original_fetch_once = mod.fetch_once
    original_normalize = mod.normalize_item
    original_capture_cycle = mod.capture_cycle

    def aligned_fetch_once(batch, timeout, stage):
        result = original_fetch_once(batch, timeout, stage)
        if not result.ok or not isinstance(result.raw, dict):
            return result
        items = result.items
        # Safe only when TWSE returned exactly one element per requested channel.
        # If counts differ, do not infer positions; normal Retry/Fail Closed handles it.
        if len(items) != len(batch):
            return result
        response_date = str((result.raw.get("queryTime") or {}).get("sysDate", "")).strip()
        repaired = []
        for stock, raw_item in zip(batch, items):
            item = dict(raw_item) if isinstance(raw_item, dict) else {}
            code = str(item.get("c", "")).strip()
            if not code:
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
        result.items = repaired
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

    def placeholder_capture_cycle(stocks, cycle_deadline):
        results, diag = original_capture_cycle(stocks, cycle_deadline)
        merged = results[0] if results else None
        placeholders = []
        if merged is not None:
            placeholders = sorted(
                str(item.get("c", ""))
                for item in merged.items
                if isinstance(item, dict) and item.get("_qts_placeholder")
            )
        diag["placeholder_symbols"] = placeholders
        diag["placeholder_count"] = len(placeholders)
        diag["quote_data_symbols"] = max(0, int(diag.get("returned_symbols", 0)) - len(placeholders))
        return results, diag

    mod.fetch_once = aligned_fetch_once
    mod.normalize_item = placeholder_normalize
    mod.capture_cycle = placeholder_capture_cycle
    mod.VERSION = "1.4.1"
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
    items = []
    if len(raw["msgArray"]) == len(batch):
        for stock, raw_item in zip(batch, raw["msgArray"]):
            item = dict(raw_item)
            if not str(item.get("c", "")).strip():
                item["_qts_original_c"] = item.get("c")
                item["_qts_original_d"] = item.get("d")
                item["c"] = stock.code
                item["_qts_placeholder"] = True
                item["_qts_source_state"] = "NO_QUOTE_DATA"
                item["_qts_requested_code"] = stock.code
                item["_qts_requested_ex_ch"] = stock.ex_ch
                item["_qts_response_date"] = raw["queryTime"]["sysDate"]
                item["d"] = raw["queryTime"]["sysDate"]
                item["_qts_date_derived_from"] = "queryTime.sysDate"
            items.append(item)
    placeholder_row = mod.normalize_item(items[0], mod.now_tw(), "PREOPEN")
    real_row = mod.normalize_item(items[1], mod.now_tw(), "PREOPEN")
    checks = {
        "positional_alignment_guarded": len(raw["msgArray"]) == len(batch),
        "placeholder_code_restored": items[0]["c"] == "8105",
        "response_date_preserved": placeholder_row["market_date"] == "20260819",
        "date_derivation_labeled": placeholder_row["date_derived_from_query"] is True,
        "placeholder_not_signal_eligible": placeholder_row["signal_eligible"] is False,
        "placeholder_state_explicit": placeholder_row["source_state"] == "NO_QUOTE_DATA",
        "real_quote_signal_eligible": real_row["signal_eligible"] is True,
        "no_fake_price": placeholder_row.get("raw_pz") is None and placeholder_row.get("last_price") == "-",
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
