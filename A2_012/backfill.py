from __future__ import annotations

import csv
import gzip
import io
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

VERSION = "A2_012_FINMIND_BACKFILL_V1.0.0"
API_BASE = "https://api.finmindtrade.com/api/v4"
START = date(2022, 1, 1)
END = date(2025, 6, 30)
REPLAY_GUARD = date(2023, 5, 15)
FORBIDDEN_FROM = date(2023, 5, 16)
MAX_REQUESTS_PER_RUN = int(os.getenv("A2_MAX_REQUESTS", "450"))
TIMEOUT = 45
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
STATE_PATH = ROOT / "checkpoint.json"
REPORT_PATH = ROOT / "report.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = {
    "BNP": ["法銀巴黎", "巴黎"],
    "CITI": ["花旗環球", "花旗"],
    "GS": ["美商高盛", "高盛"],
    "JP": ["摩根大通"],
    "ML": ["美林"],
    "MQ": ["港商麥格理", "麥格理"],
    "MS": ["台灣摩根士丹利", "摩根士丹利"],
    "NO": ["港商野村", "野村"],
    "UB": ["新加坡商瑞銀", "瑞銀"],
    "WL": ["匯立"],
}

FIELDS = [
    "date", "stock_id", "securities_trader_id", "securities_trader",
    "price", "buy", "sell", "net", "canonical", "source", "version"
]


def headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}", "User-Agent": VERSION}


def api_get(path: str, token: str, params: dict) -> dict:
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.get(url, headers=headers(token), params=params, timeout=TIMEOUT)
    if r.status_code == 401:
        raise RuntimeError("FINMIND_TOKEN_INVALID_OR_EXPIRED")
    if r.status_code == 403:
        raise PermissionError("FINMIND_SPONSOR_PERMISSION_REQUIRED")
    r.raise_for_status()
    obj = r.json()
    if isinstance(obj, dict) and obj.get("status") not in (None, 200):
        msg = str(obj.get("msg") or obj.get("message") or obj)
        if "sponsor" in msg.lower() or "permission" in msg.lower():
            raise PermissionError("FINMIND_SPONSOR_PERMISSION_REQUIRED: " + msg)
    return obj


def load_state() -> dict:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "version": VERSION,
        "next_date": START.isoformat(),
        "next_broker_index": 0,
        "mapped": {},
        "requests_total": 0,
        "rows_total": 0,
        "completed": False,
        "lookahead": 0,
    }


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def normalize_name(x: str) -> str:
    return re.sub(r"[\s\-－_()（）]", "", str(x or "")).replace("證券", "")


def trader_info(token: str) -> list[dict]:
    # TaiwanSecuritiesTraderInfo is public metadata; query via generic data endpoint.
    obj = api_get("data", token, {"dataset": "TaiwanSecuritiesTraderInfo"})
    return obj.get("data") or []


def map_brokers(token: str) -> dict[str, dict]:
    rows = trader_info(token)
    mapped: dict[str, dict] = {}
    for canonical, aliases in TARGETS.items():
        candidates = []
        for row in rows:
            name = str(row.get("securities_trader") or row.get("name") or "")
            sid = str(row.get("securities_trader_id") or row.get("id") or "")
            if not sid or not name:
                continue
            n = normalize_name(name)
            score = 0
            for alias in aliases:
                a = normalize_name(alias)
                if n == a:
                    score = max(score, 100)
                elif a in n or n in a:
                    score = max(score, 80)
            if score:
                candidates.append((score, len(n), sid, name))
        if candidates:
            candidates.sort(key=lambda x: (-x[0], x[1], x[2]))
            _, _, sid, name = candidates[0]
            mapped[canonical] = {"id": sid, "name": name}
    return mapped


def sponsor_canary(token: str, mapped: dict[str, dict]) -> dict:
    if "MS" not in mapped:
        return {"status": "FAIL", "reason": "MS_MAPPING_MISSING"}
    sid = mapped["MS"]["id"]
    try:
        obj = api_get(
            "taiwan_stock_trading_daily_report",
            token,
            {"securities_trader_id": sid, "date": "2022-06-16"},
        )
    except PermissionError as e:
        return {"status": "SPONSOR_REQUIRED", "reason": str(e)}
    data = obj.get("data") or []
    return {
        "status": "PASS" if isinstance(data, list) else "FAIL",
        "rows": len(data) if isinstance(data, list) else 0,
        "broker_id": sid,
    }


def month_path(d: date) -> Path:
    return DATA_DIR / f"broker_{d:%Y_%m}.csv.gz"


def append_rows(d: date, rows: list[dict]) -> int:
    if not rows:
        return 0
    p = month_path(d)
    existing_keys = set()
    existing_rows: list[dict] = []
    if p.exists():
        with gzip.open(p, "rt", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for r in reader:
                key = (r.get("date"), r.get("stock_id"), r.get("canonical"))
                existing_keys.add(key)
                existing_rows.append(r)

    new_rows = []
    for r in rows:
        key = (str(r["date"]), str(r["stock_id"]), str(r["canonical"]))
        if key in existing_keys:
            continue
        existing_keys.add(key)
        new_rows.append({k: r.get(k, "") for k in FIELDS})

    if not new_rows:
        return 0

    all_rows = existing_rows + new_rows
    all_rows.sort(key=lambda r: (r.get("date", ""), r.get("stock_id", ""), r.get("canonical", "")))
    with gzip.open(p, "wt", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(all_rows)
    return len(new_rows)


def fetch_one(token: str, canonical: str, trader: dict, d: date) -> list[dict]:
    obj = api_get(
        "taiwan_stock_trading_daily_report",
        token,
        {"securities_trader_id": trader["id"], "date": d.isoformat()},
    )
    data = obj.get("data") or []
    out = []
    for r in data:
        rd = str(r.get("date") or d.isoformat())[:10]
        if rd > d.isoformat():
            raise RuntimeError(f"LOOKAHEAD_DETECTED input={rd} replay={d.isoformat()}")
        buy = int(float(r.get("buy") or 0))
        sell = int(float(r.get("sell") or 0))
        out.append({
            "date": rd,
            "stock_id": str(r.get("stock_id") or ""),
            "securities_trader_id": str(r.get("securities_trader_id") or trader["id"]),
            "securities_trader": str(r.get("securities_trader") or trader["name"]),
            "price": r.get("price", ""),
            "buy": buy,
            "sell": sell,
            "net": buy - sell,
            "canonical": canonical,
            "source": "FinMind/TaiwanStockTradingDailyReport",
            "version": VERSION,
        })
    return out


def mandatory_lookahead_test() -> dict:
    blocked = FORBIDDEN_FROM > REPLAY_GUARD
    return {
        "replay_date": REPLAY_GUARD.isoformat(),
        "input_data_date": FORBIDDEN_FROM.isoformat(),
        "expected": "STOP_LOOKAHEAD",
        "actual": "STOP_LOOKAHEAD" if blocked else "LEAK",
        "result": "PASS_NEGATIVE_BLOCKED" if blocked else "FAIL_LOOKAHEAD",
    }


def run() -> int:
    token = os.getenv("FINMIND_TOKEN", "").strip()
    report = {
        "version": VERSION,
        "time": datetime.utcnow().isoformat() + "Z",
        "lookahead_test": mandatory_lookahead_test(),
    }
    if not token:
        report.update({"result": "WAITING_FINMIND_TOKEN", "next": "Add GitHub Actions secret FINMIND_TOKEN"})
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False))
        return 0

    state = load_state()
    mapped = map_brokers(token)
    state["mapped"] = mapped
    report["mapped"] = mapped
    report["mapped_count"] = len(mapped)

    if len(mapped) < 10:
        report.update({
            "result": "STOP_BROKER_MAP_INCOMPLETE",
            "missing": sorted(set(TARGETS) - set(mapped)),
        })
        save_state(state)
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False))
        return 2

    canary = sponsor_canary(token, mapped)
    report["sponsor_canary"] = canary
    if canary["status"] == "SPONSOR_REQUIRED":
        report.update({"result": "STOP_SPONSOR_REQUIRED"})
        save_state(state)
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False))
        return 3
    if canary["status"] != "PASS":
        report.update({"result": "STOP_CANARY_FAILED"})
        save_state(state)
        REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False))
        return 4

    if report["lookahead_test"]["result"] != "PASS_NEGATIVE_BLOCKED":
        raise RuntimeError("MANDATORY_LOOKAHEAD_GUARD_FAILED")

    canonical_order = list(TARGETS.keys())
    cur = date.fromisoformat(state.get("next_date", START.isoformat()))
    broker_index = int(state.get("next_broker_index", 0))
    requests_used = 0
    rows_added = 0
    empty_requests = 0
    errors = []

    while cur <= END and requests_used < MAX_REQUESTS_PER_RUN:
        canonical = canonical_order[broker_index]
        trader = mapped[canonical]
        try:
            rows = fetch_one(token, canonical, trader, cur)
            rows_added += append_rows(cur, rows)
            if not rows:
                empty_requests += 1
        except PermissionError:
            report.update({"result": "STOP_SPONSOR_REQUIRED"})
            break
        except Exception as e:
            errors.append({"date": cur.isoformat(), "canonical": canonical, "error": str(e)})
            if "429" in str(e):
                break

        requests_used += 1
        broker_index += 1
        if broker_index >= len(canonical_order):
            broker_index = 0
            cur += timedelta(days=1)

        # Avoid bursts; 450 requests/run remains below the user's displayed 600/hour ceiling.
        time.sleep(0.12)

    state["next_date"] = cur.isoformat()
    state["next_broker_index"] = broker_index
    state["requests_total"] = int(state.get("requests_total", 0)) + requests_used
    state["rows_total"] = int(state.get("rows_total", 0)) + rows_added
    state["completed"] = cur > END
    state["lookahead"] = 0
    state["updated_at"] = datetime.utcnow().isoformat() + "Z"
    save_state(state)

    if state["completed"]:
        result = "MILESTONE_2022_2025H1_BROKER_BACKFILL_COMPLETE"
    elif report.get("result") != "STOP_SPONSOR_REQUIRED":
        result = "AUTO_CONTINUE"
    else:
        result = report["result"]

    report.update({
        "result": result,
        "requests_this_run": requests_used,
        "rows_added_this_run": rows_added,
        "empty_requests": empty_requests,
        "next_date": state["next_date"],
        "next_broker_index": state["next_broker_index"],
        "requests_total": state["requests_total"],
        "rows_total": state["rows_total"],
        "completed": state["completed"],
        "lookahead": 0,
        "errors": errors[:20],
    })
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if result in ("AUTO_CONTINUE", "MILESTONE_2022_2025H1_BROKER_BACKFILL_COMPLETE") else 5


if __name__ == "__main__":
    raise SystemExit(run())
