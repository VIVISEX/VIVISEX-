from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_000_UNIVERSE_V1.2.0"
OUTDIR = Path(os.getenv("A4_UNIVERSE_DIR", "output/universe"))

PRIMARY_SHEET = os.getenv("A4_A2_EVENT_SHEET_ID", "1CVin_CtL0gVwMjL9xazfqPiPFQRluYxm7Vmrgy5mRcI")
PRIMARY_TAB = os.getenv("A4_A2_EVENT_TAB", "01_每日外資行為事件")
FALLBACK_SHEET = os.getenv("BROKER_SHEET_ID", "1MOs8_soYSsq8-UIFbDhYnBAogFX8cVSwZ-CCejNE0qM")
FALLBACK_TAB = os.getenv("BROKER_SHEET_TAB", "10_券商原始資料")
MAX_STALENESS_DAYS = int(os.getenv("A4_UNIVERSE_MAX_STALENESS_DAYS", "4"))

EVENT_HEADERS = [
    "事件ID", "資料日期", "市場別", "股票代號", "股票名稱", "買超家數", "賣超家數", "零買賣超家數",
    "總買進股數", "總賣出股數", "總買賣超股數", "總買賣超金額", "買超外資名單", "賣超外資名單",
    "共同方向", "同步級別", "計算批次", "寫入時間", "驗證狀態",
]


def _date(v: Any) -> date:
    s = str(v).strip().split()[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass
    raise ValueError(f"bad date={v}")


def _num(v: Any) -> float:
    s = str(v).strip().replace(",", "")
    return float(s) if s else 0.0


def _market(v: Any) -> str:
    s = str(v).strip().upper()
    if s in {"TWSE", "TSE", "上市"}:
        return "TSE"
    if s in {"TPEX", "OTC", "上櫃"}:
        return "OTC"
    return "UNRESOLVED"


def _valid_code(code: str) -> bool:
    c = code.strip().upper()
    return bool(c) and len(c) <= 12 and c.replace("-", "").isalnum()


def _freshness_guard(source_date: date, target: date) -> None:
    age = (target - source_date).days
    if age <= 0:
        raise RuntimeError(f"LOOKAHEAD_GUARD_FAIL source_date={source_date} target={target}")
    if age > MAX_STALENESS_DAYS:
        raise RuntimeError(f"STALE_UNIVERSE source_date={source_date} target={target} age_days={age}")


def validate_event_layout(rows: list[list[Any]]) -> dict[str, Any]:
    if not rows:
        raise RuntimeError("empty A2 event sheet")
    header = [str(x).strip() for x in rows[0]]
    missing = [h for h in EVENT_HEADERS if h not in header]
    return {"missing": missing, "pass": not missing, "header_count": len(header)}


def build_from_a2_events(rows: list[list[Any]], target: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    layout = validate_event_layout(rows)
    if not layout["pass"]:
        raise RuntimeError("A2_EVENT_SCHEMA_CHANGED:" + ",".join(layout["missing"]))
    idx = {str(h).strip(): i for i, h in enumerate(rows[0])}
    parsed: list[tuple[date, list[Any]]] = []
    bad_rows = 0
    for r in rows[1:]:
        try:
            if len(r) <= idx["驗證狀態"]:
                continue
            d = _date(r[idx["資料日期"]])
            if d >= target:
                continue
            if str(r[idx["驗證狀態"]]).strip().upper() not in {"PASS", "OK", "通過"}:
                continue
            code = str(r[idx["股票代號"]]).strip().upper()
            if not _valid_code(code):
                continue
            parsed.append((d, r))
        except Exception:
            bad_rows += 1
    if not parsed:
        raise RuntimeError("no valid prior-date A2 event rows")

    source_date = max(d for d, _ in parsed)
    _freshness_guard(source_date, target)
    selected = [r for d, r in parsed if d == source_date]
    by_code: dict[str, dict[str, Any]] = {}
    duplicate_rows = 0
    for r in selected:
        code = str(r[idx["股票代號"]]).strip().upper()
        item = {
            "market": _market(r[idx["市場別"]]),
            "code": code,
            "name": str(r[idx["股票名稱"]]).strip(),
            "source_date": source_date.isoformat(),
            "broker_buy": _num(r[idx["總買進股數"]]),
            "broker_sell": _num(r[idx["總賣出股數"]]),
            "broker_net": _num(r[idx["總買賣超股數"]]),
            "broker_count": int(_num(r[idx["買超家數"]]) + _num(r[idx["賣超家數"]]) + _num(r[idx["零買賣超家數"]])),
            "direction": str(r[idx["共同方向"]]).strip(),
            "sync_level": str(r[idx["同步級別"]]).strip(),
            "batch_id": str(r[idx["計算批次"]]).strip(),
            "dq_status": str(r[idx["驗證狀態"]]).strip(),
        }
        if code in by_code:
            duplicate_rows += 1
            old = by_code[code]
            old_ts = str(old.get("batch_id", ""))
            new_ts = item["batch_id"]
            if new_ts >= old_ts:
                by_code[code] = item
        else:
            by_code[code] = item

    universe = list(by_code.values())
    universe.sort(key=lambda x: (-abs(x["broker_net"]), x["code"]))
    markets = {"TSE": 0, "OTC": 0, "UNRESOLVED": 0}
    for x in universe:
        markets[x["market"] if x["market"] in markets else "UNRESOLVED"] += 1

    report = {
        "component": "A4_000",
        "version": VERSION,
        "source": "A2_EVIDENCE_01_每日外資行為事件",
        "target_date": target.isoformat(),
        "source_date": source_date.isoformat(),
        "source_rows": len(selected),
        "symbols": len(universe),
        "market_counts": markets,
        "duplicate_rows_resolved": duplicate_rows,
        "duplicate_free": len({x["code"] for x in universe}) == len(universe),
        "same_day_blocked": source_date < target,
        "freshness_days": (target - source_date).days,
        "bad_rows_skipped": bad_rows,
        "layout": layout,
        "pass": bool(universe) and source_date < target and markets["UNRESOLVED"] == 0,
    }
    if not report["pass"]:
        raise RuntimeError("A2_EVENT_UNIVERSE_GATE_FAIL:" + json.dumps(report, ensure_ascii=False))
    return universe, report


def build_from_legacy_raw(rows: list[list[Any]], target: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not rows or len(rows[0]) < 14:
        raise RuntimeError("legacy broker sheet width invalid")
    parsed: list[tuple[date, list[Any]]] = []
    for r in rows[1:]:
        if len(r) < 14:
            continue
        try:
            d = _date(r[0])
            code = str(r[4]).strip().upper()
            dq = str(r[13]).strip().upper()
            if d >= target or not _valid_code(code) or (dq and dq not in {"通過", "PASS", "OK"}):
                continue
            parsed.append((d, r))
        except Exception:
            continue
    if not parsed:
        raise RuntimeError("no valid legacy rows")
    source_date = max(d for d, _ in parsed)
    _freshness_guard(source_date, target)
    by_code: dict[str, dict[str, Any]] = {}
    for d, r in parsed:
        if d != source_date:
            continue
        code = str(r[4]).strip().upper()
        item = by_code.setdefault(code, {
            "market": "UNRESOLVED", "code": code, "name": str(r[5]).strip(), "source_date": source_date.isoformat(),
            "broker_buy": 0.0, "broker_sell": 0.0, "broker_net": 0.0, "broker_count": 0,
            "direction": "", "sync_level": "", "batch_id": str(r[12]).strip(), "dq_status": str(r[13]).strip(),
        })
        item["broker_buy"] += _num(r[6])
        item["broker_sell"] += _num(r[7])
        item["broker_net"] += _num(r[8])
        item["broker_count"] += 1
    universe = list(by_code.values())
    universe.sort(key=lambda x: (-abs(x["broker_net"]), x["code"]))
    report = {
        "component": "A4_000", "version": VERSION, "source": "LEGACY_BROKER_RAW_FALLBACK",
        "target_date": target.isoformat(), "source_date": source_date.isoformat(), "symbols": len(universe),
        "freshness_days": (target - source_date).days, "pass": bool(universe),
        "warning": "market resolution deferred to Shioaji contracts",
    }
    return universe, report


def _service() -> Any:
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON missing")
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build as gbuild
    info = json.loads(raw)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"])
    return gbuild("sheets", "v4", credentials=creds, cache_discovery=False)


def _fetch(svc: Any, sid: str, tab: str, rng: str) -> list[list[Any]]:
    return svc.spreadsheets().values().get(
        spreadsheetId=sid, range=f"'{tab}'!{rng}", majorDimension="ROWS"
    ).execute().get("values", [])


def fetch_and_build(target: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    svc = _service()
    primary_error = None
    try:
        rows = _fetch(svc, PRIMARY_SHEET, PRIMARY_TAB, "A:S")
        return build_from_a2_events(rows, target)
    except Exception as exc:
        primary_error = f"{type(exc).__name__}: {exc}"
    rows = _fetch(svc, FALLBACK_SHEET, FALLBACK_TAB, "A:N")
    universe, report = build_from_legacy_raw(rows, target)
    report["primary_error"] = primary_error
    return universe, report


def write(universe: list[dict[str, Any]], report: dict[str, Any]) -> None:
    OUTDIR.mkdir(parents=True, exist_ok=True)
    fields = [
        "market", "code", "name", "source_date", "broker_buy", "broker_sell", "broker_net", "broker_count",
        "direction", "sync_level", "batch_id", "dq_status",
    ]
    with (OUTDIR / "stocks.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows([{k: row.get(k, "") for k in fields} for row in universe])
    (OUTDIR / "universe.json").write_text(
        json.dumps({"report": report, "symbols": universe}, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def selftest() -> int:
    rows = [
        EVENT_HEADERS,
        ["E1", "2026-08-18", "TWSE", "2330", "台積電", "6", "2", "0", "100", "20", "80", "0", "A", "B", "共同買超", "6家同步", "B1", "2026-08-18 20:00:00", "PASS"],
        ["E2", "2026-08-18", "TPEx", "6488", "環球晶", "2", "3", "0", "30", "60", "-30", "0", "A", "B", "共同賣超", "未達6家", "B1", "2026-08-18 20:00:00", "PASS"],
        ["E3", "2026-08-19", "TWSE", "2317", "鴻海", "9", "0", "0", "999", "0", "999", "0", "A", "", "共同買超", "9家同步", "B2", "2026-08-19 08:00:00", "PASS"],
    ]
    uni, rep = build_from_a2_events(rows, date(2026, 8, 19))
    checks = {
        "latest_prior_date_only": all(x["source_date"] == "2026-08-18" for x in uni),
        "lookahead_blocked": all(x["code"] != "2317" for x in uni),
        "twse_map": next(x for x in uni if x["code"] == "2330")["market"] == "TSE",
        "tpex_map": next(x for x in uni if x["code"] == "6488")["market"] == "OTC",
        "dq_pass": all(x["dq_status"] == "PASS" for x in uni),
    }
    rep["checks"] = checks
    rep["pass"] = all(checks.values())
    write(uni, rep)
    print(json.dumps(rep, ensure_ascii=False))
    return 0 if rep["pass"] else 2


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--target-date")
    a = p.parse_args()
    if a.self_test:
        return selftest()
    target = _date(a.target_date) if a.target_date else datetime.now(TZ).date()
    universe, report = fetch_and_build(target)
    write(universe, report)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report.get("pass") else 3


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        OUTDIR.mkdir(parents=True, exist_ok=True)
        err = {"component": "A4_000", "version": VERSION, "pass": False, "error": f"{type(exc).__name__}: {exc}"}
        (OUTDIR / "failure.json").write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(err, ensure_ascii=False), file=sys.stderr)
        sys.exit(99)
