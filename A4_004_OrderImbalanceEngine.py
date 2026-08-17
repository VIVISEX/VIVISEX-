from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_004_OI_V1.0.0"
INPUT_VERSION_PREFIX = "A4_003_CLEAN_"
EPS = 1e-12


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _sum_levels(row: dict[str, Any], side: str) -> tuple[float | None, int]:
    vals: list[float] = []
    for i in range(1, 6):
        x = _num(row.get(f"{side}_volume_{i}"))
        if x is not None and x >= 0:
            vals.append(x)
    if len(vals) != 5:
        return None, len(vals)
    return sum(vals), 5


def compute_oi(row: dict[str, Any]) -> dict[str, Any]:
    out = dict(row)
    flags: list[str] = []

    if not str(row.get("VERSION", "")).startswith(INPUT_VERSION_PREFIX):
        flags.append("INPUT_VERSION_MISMATCH")
    if row.get("SIGNAL_ELIGIBLE") is not True:
        flags.append("UPSTREAM_BLOCKED")
    if str(row.get("DQ_STATUS", "")) != "OK":
        flags.append("UPSTREAM_DQ")
    if str(row.get("phase", "")) not in {"PREOPEN", "OPEN_VALIDATION"}:
        flags.append("INVALID_PHASE")

    bid, bid_n = _sum_levels(row, "bid")
    ask, ask_n = _sum_levels(row, "ask")
    if bid_n != 5 or ask_n != 5:
        flags.append("INCOMPLETE_BOOK")

    denom = None if bid is None or ask is None else bid + ask
    if denom is None or denom <= EPS:
        flags.append("ZERO_OR_INVALID_DEPTH")
        oi = None
        signed = None
    else:
        oi = bid / denom
        signed = (bid - ask) / denom

    if oi is None:
        state = "BLOCKED"
    elif oi >= 0.65:
        state = "BUY_DOMINANT"
    elif oi <= 0.35:
        state = "SELL_DOMINANT"
    else:
        state = "BALANCED"

    flags = list(dict.fromkeys(flags))
    eligible = len(flags) == 0
    out.update({
        "OI_VERSION": VERSION,
        "bid_depth_5": bid,
        "ask_depth_5": ask,
        "order_imbalance": oi,
        "order_imbalance_signed": signed,
        "oi_state": state if eligible else "BLOCKED",
        "OI_DQ_STATUS": "OK" if eligible else "|".join(flags),
        "OI_SIGNAL_ELIGIBLE": eligible,
    })
    return out


def run_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    output = [compute_oi(r) for r in rows]
    report = {
        "component": "A4_004",
        "version": VERSION,
        "checked_at": datetime.now(TZ).isoformat(),
        "input_rows": len(rows),
        "output_rows": len(output),
        "eligible_rows": sum(1 for r in output if r["OI_SIGNAL_ELIGIBLE"]),
        "blocked_rows": sum(1 for r in output if not r["OI_SIGNAL_ELIGIBLE"]),
        "pass": len(output) == len(rows),
    }
    return output, report


def self_test() -> int:
    base = {
        "VERSION": "A4_003_CLEAN_V1.0.1", "SIGNAL_ELIGIBLE": True,
        "DQ_STATUS": "OK", "phase": "PREOPEN", "code": "2330",
    }
    for i, v in enumerate([100, 100, 100, 100, 100], 1):
        base[f"bid_volume_{i}"] = v
    for i, v in enumerate([50, 50, 50, 50, 50], 1):
        base[f"ask_volume_{i}"] = v

    buy = compute_oi(base)
    sell_input = dict(base)
    for i in range(1, 6):
        sell_input[f"bid_volume_{i}"] = 20
        sell_input[f"ask_volume_{i}"] = 100
    sell = compute_oi(sell_input)
    balanced_input = dict(base)
    for i in range(1, 6):
        balanced_input[f"bid_volume_{i}"] = 100
        balanced_input[f"ask_volume_{i}"] = 100
    balanced = compute_oi(balanced_input)
    partial = dict(base); partial["ask_volume_5"] = None
    upstream = dict(base); upstream["SIGNAL_ELIGIBLE"] = False; upstream["DQ_STATUS"] = "SOURCE_GAP"
    zero = dict(base)
    for i in range(1, 6): zero[f"bid_volume_{i}"] = zero[f"ask_volume_{i}"] = 0
    badver = dict(base); badver["VERSION"] = "UNKNOWN"

    checks = {
        "buy_formula": abs(buy["order_imbalance"] - (500/750)) < 1e-12,
        "buy_classification": buy["oi_state"] == "BUY_DOMINANT",
        "sell_classification": sell["oi_state"] == "SELL_DOMINANT",
        "balanced_classification": balanced["oi_state"] == "BALANCED" and abs(balanced["order_imbalance"] - 0.5) < 1e-12,
        "signed_range": -1 <= sell["order_imbalance_signed"] <= 1 and -1 <= buy["order_imbalance_signed"] <= 1,
        "partial_book_blocked": compute_oi(partial)["OI_SIGNAL_ELIGIBLE"] is False,
        "source_gap_blocked": compute_oi(upstream)["OI_SIGNAL_ELIGIBLE"] is False,
        "zero_depth_blocked": compute_oi(zero)["OI_SIGNAL_ELIGIBLE"] is False,
        "version_guard": compute_oi(badver)["OI_SIGNAL_ELIGIBLE"] is False,
        "no_input_mutation": "OI_VERSION" not in base,
    }
    report = {"component":"A4_004","version":VERSION,"mode":"self_test","checked_at":datetime.now(TZ).isoformat(),"checks":checks,"pass":all(checks.values())}
    Path("status/a4").mkdir(parents=True, exist_ok=True)
    Path("status/a4/a4_004_oi_selftest_latest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 2


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--input")
    p.add_argument("--output")
    args = p.parse_args()
    if args.self_test:
        return self_test()
    if not args.input or not args.output:
        p.error("--input and --output are required unless --self-test")
    rows = [json.loads(line) for line in Path(args.input).read_text(encoding="utf-8").splitlines() if line.strip()]
    output, report = run_rows(rows)
    out = Path(args.output); out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in output) + ("\n" if output else ""), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 3


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"component":"A4_004","pass":False,"error":f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        sys.exit(99)
