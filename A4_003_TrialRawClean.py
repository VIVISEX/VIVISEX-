from __future__ import annotations

import argparse
import hashlib
import json
import math
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_003_CLEAN_V1.0.0"
STRATEGY_ID = "QTS-WP-A4-PREOPEN-V1"
FUND_ID = "N/A"


@dataclass(frozen=True)
class CleanResult:
    rows: list[dict[str, Any]]
    report: dict[str, Any]


def _scalar(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
            return None
        return value
    if isinstance(value, (tuple, list)):
        return list(value)
    s = str(value).strip()
    return s if s != "" else None


def _num(value: Any, integer: bool = False) -> int | float | None:
    value = _scalar(value)
    if value is None:
        return None
    try:
        return int(float(value)) if integer else float(value)
    except (TypeError, ValueError):
        return None


def _bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "t", "yes", "y", "simtrade", "simulated"}:
        return True
    if s in {"0", "false", "f", "no", "n", "normal"}:
        return False
    return None


def _parse_iso_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if not s:
            return None
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    return dt.astimezone(TZ)


def _parse_market_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, (tuple, list)) and len(value) >= 3:
        try:
            return date(int(value[0]), int(value[1]), int(value[2]))
        except Exception:
            return None
    s = str(value).strip().replace("/", "-")
    if not s:
        return None
    if s.isdigit() and len(s) == 8:
        s = f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        return None


def _parse_market_time(value: Any) -> time | None:
    if value is None:
        return None
    if isinstance(value, time):
        return value
    if isinstance(value, (tuple, list)) and len(value) >= 3:
        vals = list(value) + [0]
        try:
            return time(int(vals[0]), int(vals[1]), int(vals[2]), int(vals[3]))
        except Exception:
            return None
    s = str(value).strip()
    if not s:
        return None
    try:
        return time.fromisoformat(s)
    except ValueError:
        return None


def _combine_market_dt(d: date | None, t: time | None) -> datetime | None:
    if d is None or t is None:
        return None
    return datetime.combine(d, t, TZ)


def _phase(event_at: datetime | None, fallback: Any = None) -> str:
    if event_at is None:
        v = str(fallback or "").strip().upper()
        return v if v in {"PREOPEN", "OPEN_VALIDATION"} else "UNKNOWN"
    tm = event_at.time()
    if time(8, 30) <= tm < time(9, 0):
        return "PREOPEN"
    if time(9, 0) <= tm < time(9, 30):
        return "OPEN_VALIDATION"
    return "OUT_OF_WINDOW"


def _levels_from_twse_item(item: dict[str, Any]) -> dict[str, Any]:
    def split5(key: str, integer: bool = False) -> list[Any]:
        raw = item.get(key)
        if raw is None:
            return [None] * 5
        vals = [x.strip() for x in str(raw).split("_") if x.strip()]
        converted = [_num(v, integer=integer) for v in vals[:5]]
        return converted + [None] * (5 - len(converted))

    bp = split5("b")
    bv = split5("g", True)
    ap = split5("a")
    av = split5("f", True)
    out: dict[str, Any] = {}
    for i in range(5):
        n = i + 1
        out[f"bid_price_{n}"] = bp[i]
        out[f"bid_volume_{n}"] = bv[i]
        out[f"ask_price_{n}"] = ap[i]
        out[f"ask_volume_{n}"] = av[i]
        out[f"diff_bid_vol_{n}"] = None
        out[f"diff_ask_vol_{n}"] = None
    return out


def _levels_from_flat(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for i in range(1, 6):
        out[f"bid_price_{i}"] = _num(row.get(f"bid_price_{i}"))
        out[f"bid_volume_{i}"] = _num(row.get(f"bid_volume_{i}"), True)
        out[f"ask_price_{i}"] = _num(row.get(f"ask_price_{i}"))
        out[f"ask_volume_{i}"] = _num(row.get(f"ask_volume_{i}"), True)
        out[f"diff_bid_vol_{i}"] = _num(row.get(f"diff_bid_vol_{i}"), True)
        out[f"diff_ask_vol_{i}"] = _num(row.get(f"diff_ask_vol_{i}"), True)
    return out


def _detect_source(record: dict[str, Any], source_hint: str | None = None) -> str:
    hinted = str(source_hint or record.get("source") or "").strip().upper()
    if hinted in {"TWSE", "TWSE_MIS", "MIS"}:
        return "TWSE_MIS"
    if hinted == "SHIOAJI":
        return "SHIOAJI"
    if "raw" in record and isinstance(record.get("raw"), dict):
        return "TWSE_MIS"
    if "msgArray" in record or any(k in record for k in ("b", "a", "g", "f", "ts")):
        return "TWSE_MIS"
    if any(k in record for k in ("diff_bid_vol_1", "simtrade", "event_type")):
        return "SHIOAJI"
    return "UNKNOWN"


def expand_input(record: dict[str, Any], source_hint: str | None = None) -> list[dict[str, Any]]:
    source = _detect_source(record, source_hint)
    if source == "TWSE_MIS" and isinstance(record.get("raw"), dict):
        envelope = record
        raw = envelope["raw"]
        out = []
        for item in raw.get("msgArray", []) or []:
            out.append({"__source": "TWSE_MIS", "__envelope": envelope, "__item": item})
        if not out and str(envelope.get("dq_status", "")).upper() == "SOURCE_GAP":
            out.append({"__source": "TWSE_MIS", "__envelope": envelope, "__item": {}})
        return out
    if source == "TWSE_MIS" and "msgArray" in record:
        return [
            {"__source": "TWSE_MIS", "__envelope": record, "__item": item}
            for item in (record.get("msgArray") or [])
        ]
    return [{"__source": source, "__envelope": record, "__item": record}]


def normalize_record(
    expanded: dict[str, Any],
    *,
    run_id: str,
    expected_as_of_date: date | None = None,
) -> dict[str, Any]:
    source = expanded["__source"]
    envelope = expanded["__envelope"]
    item = expanded["__item"]

    captured_at = _parse_iso_dt(envelope.get("captured_at") or item.get("captured_at"))
    code = str(item.get("c") or item.get("code") or "").strip()

    if source == "TWSE_MIS" and any(k in item for k in ("b", "a", "g", "f", "ts")):
        market_date = _parse_market_date(item.get("d"))
        market_time = _parse_market_time(item.get("t") or item.get("%"))
        levels = _levels_from_twse_item(item)
        observed_price = _num(item.get("z") or item.get("pz"))
        single_volume = _num(item.get("tv"), True)
        total_volume = _num(item.get("v"), True)
        simulated = _bool(item.get("ts"))
        market = str(item.get("ex") or "").upper() or None
        source_gap = str(envelope.get("dq_status", "")).upper() == "SOURCE_GAP"
    else:
        market_date = _parse_market_date(item.get("market_date") or item.get("date"))
        market_time = _parse_market_time(item.get("market_time") or item.get("time"))
        levels = _levels_from_flat(item)
        observed_price = _num(item.get("last_price") if source == "TWSE_MIS" else item.get("close"))
        single_volume = _num(item.get("single_volume") if source == "TWSE_MIS" else item.get("volume"), True)
        total_volume = _num(item.get("total_volume"), True)
        simulated = _bool(item.get("simulated_flag") if source == "TWSE_MIS" else item.get("simtrade"))
        market = str(item.get("market") or item.get("exchange") or "").upper() or None
        source_gap = str(item.get("dq_status", "")).upper() == "SOURCE_GAP"

    event_at = _combine_market_dt(market_date, market_time)
    available_at = captured_at or event_at
    phase = _phase(event_at, envelope.get("phase") or item.get("phase"))
    as_of = available_at.date() if available_at else expected_as_of_date

    dq: list[str] = []
    if source == "UNKNOWN":
        dq.append("UNKNOWN_SOURCE")
    if source_gap:
        dq.append("SOURCE_GAP")
    if not code and not source_gap:
        dq.append("MISSING_SYMBOL")
    if event_at is None and not source_gap:
        dq.append("INVALID_MARKET_TIMESTAMP")
    if available_at is None:
        dq.append("INVALID_AVAILABLE_AT")
    if market_date and as_of and market_date > as_of:
        dq.append("LOOKAHEAD_VIOLATION")
    if expected_as_of_date and market_date and market_date != expected_as_of_date:
        dq.append("STALE_DATE" if market_date < expected_as_of_date else "LOOKAHEAD_VIOLATION")

    book_fields = [
        levels[f"{side}_{kind}_{i}"]
        for side in ("bid", "ask")
        for kind in ("price", "volume")
        for i in range(1, 6)
    ]
    book_complete = all(v is not None for v in book_fields)
    if phase == "PREOPEN" and not source_gap and not book_complete:
        dq.append("PARTIAL_BOOK")

    dq = list(dict.fromkeys(dq))
    dq_status = "OK" if not dq else "|".join(dq)
    signal_eligible = dq_status == "OK" and phase in {"PREOPEN", "OPEN_VALIDATION"}

    available_iso = available_at.isoformat(timespec="milliseconds") if available_at else None
    event_iso = event_at.isoformat(timespec="microseconds") if event_at else None
    source_date = market_date.isoformat() if market_date else None
    as_of_date = as_of.isoformat() if as_of else None
    key_material = "|".join(
        [source, source_date or "N/A", code or "N/A", event_iso or "N/A", available_iso or "N/A"]
    )
    data_key = hashlib.sha256(key_material.encode("utf-8")).hexdigest()

    row: dict[str, Any] = {
        "DATA_KEY": data_key,
        "SOURCE": source,
        "SOURCE_DATE": source_date,
        "AVAILABLE_AT": available_iso,
        "AS_OF_DATE": as_of_date,
        "VERSION": VERSION,
        "RUN_ID": run_id,
        "STRATEGY_ID": STRATEGY_ID,
        "FUND_ID": FUND_ID,
        "event_at": event_iso,
        "phase": phase,
        "market": market,
        "code": code or None,
        "simulated": simulated,
        "observed_price": observed_price,
        "single_volume": single_volume,
        "total_volume": total_volume,
        "book_complete": book_complete,
        "source_gap": source_gap,
        "DQ_STATUS": dq_status,
        "SIGNAL_ELIGIBLE": signal_eligible,
    }
    row.update(levels)
    return row


def clean_records(
    records: Iterable[dict[str, Any]],
    *,
    run_id: str | None = None,
    expected_as_of_date: date | None = None,
) -> CleanResult:
    run_id = run_id or f"A4_003_{datetime.now(TZ).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"
    normalized: list[dict[str, Any]] = []
    input_records = 0
    for record in records:
        input_records += 1
        for expanded in expand_input(record):
            normalized.append(
                normalize_record(expanded, run_id=run_id, expected_as_of_date=expected_as_of_date)
            )

    normalized.sort(key=lambda r: (r.get("AVAILABLE_AT") or "", r.get("SOURCE") or "", r.get("code") or ""))
    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    duplicate_count = 0
    for row in normalized:
        key = row["DATA_KEY"]
        if key in seen:
            duplicate_count += 1
            continue
        seen.add(key)
        deduped.append(row)

    dq_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    for row in deduped:
        source_counts[row["SOURCE"]] = source_counts.get(row["SOURCE"], 0) + 1
        for flag in str(row["DQ_STATUS"]).split("|"):
            dq_counts[flag] = dq_counts.get(flag, 0) + 1

    report = {
        "component": "A4_003",
        "version": VERSION,
        "run_id": run_id,
        "input_records": input_records,
        "expanded_rows": len(normalized),
        "output_rows": len(deduped),
        "exact_duplicates_removed": duplicate_count,
        "source_counts": source_counts,
        "dq_counts": dq_counts,
        "signal_eligible_rows": sum(1 for r in deduped if r["SIGNAL_ELIGIBLE"]),
        "source_gap_rows": sum(1 for r in deduped if r["source_gap"]),
        "pass": True,
    }
    return CleanResult(rows=deduped, report=report)


def read_ndjson(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for lineno, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError(f"line {lineno}: JSON object required")
            rows.append(obj)
    return rows


def write_ndjson(path: Path, rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


def self_test() -> dict[str, Any]:
    d = date(2026, 8, 17)
    captured = "2026-08-17T08:45:00.500+08:00"
    twse_item = {
        "ex": "tse", "c": "2330", "d": "20260817", "t": "08:45:00",
        "ts": "1", "z": "1234.5", "tv": "10", "v": "100",
        "b": "1234_1233.5_1233_1232.5_1232_", "g": "10_20_30_40_50_",
        "a": "1235_1235.5_1236_1236.5_1237_", "f": "11_21_31_41_51_",
    }
    twse_env = {
        "captured_at": captured, "phase": "PREOPEN",
        "raw": {"rtcode": "0000", "msgArray": [twse_item]},
    }
    shioaji = {
        "source": "SHIOAJI", "captured_at": "2026-08-17T08:45:00.600+08:00",
        "event_type": "bidask_stk", "exchange": "TSE", "code": "2330",
        "market_date": [2026, 8, 17], "market_time": [8, 45, 0, 550000],
        "simtrade": True, "close": 1234.5, "volume": 10, "total_volume": 100,
    }
    for i in range(1, 6):
        shioaji[f"bid_price_{i}"] = 1234.5 - i * 0.5
        shioaji[f"bid_volume_{i}"] = i * 10
        shioaji[f"ask_price_{i}"] = 1234.5 + i * 0.5
        shioaji[f"ask_volume_{i}"] = i * 11
        shioaji[f"diff_bid_vol_{i}"] = i
        shioaji[f"diff_ask_vol_{i}"] = -i

    normal = clean_records([twse_env, shioaji], run_id="SELFTEST", expected_as_of_date=d)
    duplicate = clean_records([twse_env, twse_env], run_id="SELFTEST", expected_as_of_date=d)

    partial_item = dict(twse_item)
    partial_item["b"] = "1234_"
    partial = clean_records([
        {"captured_at": captured, "phase": "PREOPEN", "raw": {"msgArray": [partial_item]}}
    ], run_id="SELFTEST", expected_as_of_date=d)

    stale_item = dict(twse_item)
    stale_item["d"] = "20260814"
    stale = clean_records([
        {"captured_at": captured, "phase": "PREOPEN", "raw": {"msgArray": [stale_item]}}
    ], run_id="SELFTEST", expected_as_of_date=d)

    future_item = dict(twse_item)
    future_item["d"] = "20260818"
    future = clean_records([
        {"captured_at": captured, "phase": "PREOPEN", "raw": {"msgArray": [future_item]}}
    ], run_id="SELFTEST", expected_as_of_date=d)

    source_gap = clean_records([
        {"captured_at": captured, "phase": "PREOPEN", "dq_status": "SOURCE_GAP", "raw": {"msgArray": []}}
    ], run_id="SELFTEST", expected_as_of_date=d)

    open_item = dict(twse_item)
    open_item["t"] = "09:05:00"
    open_phase = clean_records([
        {"captured_at": "2026-08-17T09:05:00.100+08:00", "raw": {"msgArray": [open_item]}}
    ], run_id="SELFTEST", expected_as_of_date=d)

    shuffled = clean_records([
        {"source": "SHIOAJI", **shioaji, "captured_at": "2026-08-17T08:46:00+08:00"},
        twse_env,
    ], run_id="SELFTEST", expected_as_of_date=d)

    missing_symbol = dict(shioaji)
    missing_symbol["code"] = ""
    missing = clean_records([missing_symbol], run_id="SELFTEST", expected_as_of_date=d)

    checks = {
        "dual_source_normalized": len(normal.rows) == 2 and {r["SOURCE"] for r in normal.rows} == {"TWSE_MIS", "SHIOAJI"},
        "traceability_complete": all(all(r.get(k) is not None for k in ["DATA_KEY", "SOURCE", "SOURCE_DATE", "AVAILABLE_AT", "AS_OF_DATE", "VERSION", "RUN_ID", "STRATEGY_ID", "FUND_ID"]) for r in normal.rows),
        "five_level_complete": all(r["book_complete"] for r in normal.rows),
        "duplicate_removed": duplicate.report["exact_duplicates_removed"] == 1 and len(duplicate.rows) == 1,
        "partial_book_blocked": "PARTIAL_BOOK" in partial.rows[0]["DQ_STATUS"] and not partial.rows[0]["SIGNAL_ELIGIBLE"],
        "stale_date_blocked": "STALE_DATE" in stale.rows[0]["DQ_STATUS"] and not stale.rows[0]["SIGNAL_ELIGIBLE"],
        "lookahead_blocked": "LOOKAHEAD_VIOLATION" in future.rows[0]["DQ_STATUS"] and not future.rows[0]["SIGNAL_ELIGIBLE"],
        "source_gap_preserved": source_gap.rows[0]["source_gap"] and "SOURCE_GAP" in source_gap.rows[0]["DQ_STATUS"],
        "phase_transition": open_phase.rows[0]["phase"] == "OPEN_VALIDATION",
        "chronological_sort": shuffled.rows[0]["AVAILABLE_AT"] <= shuffled.rows[-1]["AVAILABLE_AT"],
        "missing_symbol_blocked": "MISSING_SYMBOL" in missing.rows[0]["DQ_STATUS"] and not missing.rows[0]["SIGNAL_ELIGIBLE"],
        "shioaji_diff_volume_preserved": normal.rows[1 if normal.rows[1]["SOURCE"] == "SHIOAJI" else 0]["diff_bid_vol_5"] == 5,
    }
    passed = all(checks.values())
    return {
        "component": "A4_003",
        "version": VERSION,
        "mode": "self_test",
        "checked_at": datetime.now(TZ).isoformat(),
        "checks": checks,
        "pass": passed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--input", type=Path)
    parser.add_argument("--output", type=Path, default=Path("output/a4_003/clean.ndjson"))
    parser.add_argument("--report", type=Path, default=Path("output/a4_003/report.json"))
    parser.add_argument("--as-of-date", type=date.fromisoformat)
    parser.add_argument("--run-id")
    args = parser.parse_args()

    if args.self_test:
        report = self_test()
        print(json.dumps(report, ensure_ascii=False))
        return 0 if report["pass"] else 2

    if args.input is None:
        parser.error("--input is required unless --self-test is used")
    result = clean_records(read_ndjson(args.input), run_id=args.run_id, expected_as_of_date=args.as_of_date)
    write_ndjson(args.output, result.rows)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result.report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(result.report, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"component": "A4_003", "pass": False, "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        sys.exit(99)
