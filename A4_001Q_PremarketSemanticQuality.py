from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_001Q_PREMARKET_QUALITY_V1.0.0"


def num(v: Any) -> float | None:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return None
    s = str(v).strip()
    if not s or s in {"-", "--", "nan", "None"}:
        return None
    try:
        x = float(s)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def sim_flag(v: Any) -> bool:
    x = num(v)
    return x == 1.0


def parse_hms(v: Any) -> time | None:
    if v is None:
        return None
    try:
        return time.fromisoformat(str(v).strip())
    except ValueError:
        return None


def parse_date(v: Any) -> date | None:
    if v is None:
        return None
    s = str(v).strip().replace("-", "")
    if len(s) >= 8 and s[:8].isdigit():
        try:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            return None
    return None


def row_quality(row: dict[str, Any], expected_date: date) -> dict[str, Any]:
    phase = str(row.get("phase") or "").upper()
    market_date = parse_date(row.get("market_date"))
    market_time = parse_hms(row.get("market_time"))
    simulated = sim_flag(row.get("simulated_flag"))

    indicative_price = num(row.get("raw_pz")) if phase == "PREOPEN" and simulated else None
    indicative_volume = num(row.get("raw_ps")) if phase == "PREOPEN" and simulated else None
    actual_last_price = num(row.get("last_price")) if phase == "OPEN_VALIDATION" else None
    actual_open = num(row.get("open")) if phase == "OPEN_VALIDATION" else None

    book_fields = [row.get(f"{side}_{kind}_{i}") for side in ("bid", "ask") for kind in ("price", "volume") for i in range(1, 6)]
    book_complete = all(num(v) is not None for v in book_fields)

    flags: list[str] = []
    fresh_date = market_date == expected_date
    if not fresh_date:
        flags.append("STALE_OR_INVALID_MARKET_DATE")

    preopen_ready = False
    if phase == "PREOPEN":
        if market_time is None or market_time < time(8, 30):
            flags.append("PREOPEN_NOT_READY")
        elif not simulated:
            flags.append("PREOPEN_NOT_SIMULATED")
        elif indicative_price is None:
            flags.append("MISSING_INDICATIVE_PRICE")
        else:
            preopen_ready = fresh_date
        if preopen_ready and not book_complete:
            flags.append("PARTIAL_BOOK")

    if phase == "OPEN_VALIDATION" and market_time is None:
        flags.append("INVALID_OPEN_MARKET_TIME")

    flags = list(dict.fromkeys(flags))
    semantic_ok = len(flags) == 0
    signal_eligible = phase == "PREOPEN" and preopen_ready and book_complete and semantic_ok

    return {
        **row,
        "QUALITY_VERSION": VERSION,
        "INDICATIVE_PRICE": indicative_price,
        "INDICATIVE_VOLUME": indicative_volume,
        "ACTUAL_LAST_PRICE": actual_last_price,
        "ACTUAL_OPEN": actual_open,
        "PREOPEN_READY": preopen_ready,
        "BOOK_COMPLETE": book_complete,
        "SEMANTIC_DQ_STATUS": "OK" if semantic_ok else "|".join(flags),
        "PREOPEN_SIGNAL_ELIGIBLE": signal_eligible,
    }


def audit_dataframe(df: pd.DataFrame, expected_date: date) -> tuple[pd.DataFrame, dict[str, Any]]:
    rows = [row_quality(r, expected_date) for r in df.to_dict(orient="records")]
    out = pd.DataFrame(rows)
    pre = out[out["phase"].astype(str).str.upper() == "PREOPEN"].copy()
    symbol_stats: dict[str, Any] = {}
    for code, g in pre.groupby(pre["code"].astype(str)):
        ready = g[g["PREOPEN_READY"] == True]
        eligible = g[g["PREOPEN_SIGNAL_ELIGIBLE"] == True]
        symbol_stats[code] = {
            "rows": int(len(g)),
            "ready_rows": int(len(ready)),
            "eligible_rows": int(len(eligible)),
            "ready_coverage": (len(ready) / len(g)) if len(g) else 0.0,
            "eligible_coverage": (len(eligible) / len(g)) if len(g) else 0.0,
            "first_ready_capture": str(ready.iloc[0]["captured_at"]) if len(ready) else None,
        }
    report = {
        "component": "A4_001Q",
        "version": VERSION,
        "expected_date": expected_date.isoformat(),
        "checked_at": datetime.now(TZ).isoformat(),
        "rows": int(len(out)),
        "preopen_rows": int(len(pre)),
        "preopen_ready_rows": int((pre["PREOPEN_READY"] == True).sum()) if len(pre) else 0,
        "preopen_signal_eligible_rows": int((pre["PREOPEN_SIGNAL_ELIGIBLE"] == True).sum()) if len(pre) else 0,
        "preopen_not_ready_rows": int(pre["SEMANTIC_DQ_STATUS"].astype(str).str.contains("PREOPEN_NOT_READY", regex=False).sum()) if len(pre) else 0,
        "partial_book_rows": int(pre["SEMANTIC_DQ_STATUS"].astype(str).str.contains("PARTIAL_BOOK", regex=False).sum()) if len(pre) else 0,
        "symbol_stats": symbol_stats,
        "pass": True,
    }
    return out, report


def self_test() -> int:
    cols = {f"{side}_{kind}_{i}": 1 for side in ("bid", "ask") for kind in ("price", "volume") for i in range(1, 6)}
    base = {"captured_at":"2026-08-17T08:30:15+08:00","phase":"PREOPEN","code":"2330","market_date":"20260817","market_time":"08:30:07","simulated_flag":1,"last_price":"-","raw_pz":"2410.0000","raw_ps":"4","open":"-",**cols}
    good = row_quality(base, date(2026,8,17))
    early = dict(base, market_time="07:50:00", simulated_flag=None, raw_pz=None, raw_ps=None)
    partial = dict(base); partial["ask_price_5"] = None
    open_row = dict(base, phase="OPEN_VALIDATION", market_time="09:00:07", simulated_flag=0, last_price="2410", open="2410")
    stale = dict(base, market_date="20260816")
    checks = {
        "pz_is_indicative_price": good["INDICATIVE_PRICE"] == 2410.0,
        "dash_last_price_does_not_mask_pz": good["INDICATIVE_PRICE"] is not None,
        "ps_is_indicative_volume": good["INDICATIVE_VOLUME"] == 4.0,
        "normal_preopen_is_ready": good["PREOPEN_READY"] is True,
        "early_cache_is_not_ready": row_quality(early, date(2026,8,17))["PREOPEN_READY"] is False,
        "partial_book_is_blocked": row_quality(partial, date(2026,8,17))["PREOPEN_SIGNAL_ELIGIBLE"] is False,
        "open_fields_separated": row_quality(open_row, date(2026,8,17))["ACTUAL_OPEN"] == 2410.0,
        "stale_date_blocked": row_quality(stale, date(2026,8,17))["PREOPEN_SIGNAL_ELIGIBLE"] is False,
    }
    report = {"component":"A4_001Q","version":VERSION,"mode":"self_test","checks":checks,"pass":all(checks.values())}
    Path("status/a4").mkdir(parents=True, exist_ok=True)
    Path("status/a4/a4_001q_semantic_selftest_latest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 2


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--input")
    p.add_argument("--output")
    p.add_argument("--report")
    p.add_argument("--date")
    a = p.parse_args()
    if a.self_test:
        return self_test()
    if not all([a.input, a.output, a.report, a.date]):
        p.error("--input --output --report --date are required")
    expected = date.fromisoformat(a.date)
    df = pd.read_csv(a.input)
    out, report = audit_dataframe(df, expected)
    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(a.output, index=False)
    Path(a.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"component":"A4_001Q","pass":False,"error":f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        sys.exit(99)
