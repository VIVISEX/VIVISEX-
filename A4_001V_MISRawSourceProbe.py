from __future__ import annotations

import json
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

TZ = ZoneInfo("Asia/Taipei")
MIS_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
HOME = "https://mis.twse.com.tw/stock/index.jsp"
FIBEST = "https://mis.twse.com.tw/stock/fibest.jsp"
OUT = Path("output/mis_source_probe")
VERSION = "A4_001V_MIS_RAW_PROBE_V1.0.0"


def session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36 QTS-A4-RAW-PROBE/1.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
        "Referer": FIBEST,
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })
    for u in (HOME, FIBEST):
        try:
            s.get(u, timeout=3)
        except Exception:
            pass
    return s


def one(s: requests.Session, channels: list[str]) -> dict:
    started = time.perf_counter()
    try:
        r = s.get(MIS_URL, params={
            "ex_ch": "|".join(channels),
            "json": "1",
            "delay": "0",
            "_": str(int(time.time() * 1000)),
        }, timeout=4)
        text = r.text
        obj = None
        try:
            obj = r.json()
        except Exception:
            pass
        items = obj.get("msgArray", []) if isinstance(obj, dict) else []
        return {
            "at": datetime.now(TZ).isoformat(),
            "channels": channels,
            "status": r.status_code,
            "elapsed_seconds": round(time.perf_counter() - started, 4),
            "rtcode": obj.get("rtcode") if isinstance(obj, dict) else None,
            "rtmessage": obj.get("rtmessage") if isinstance(obj, dict) else None,
            "returned_codes": [str(x.get("c", "")) for x in items if isinstance(x, dict)],
            "query_time": obj.get("queryTime") if isinstance(obj, dict) else None,
            "raw_json": obj,
            "raw_text_prefix": text[:1000] if obj is None else None,
        }
    except Exception as exc:
        return {
            "at": datetime.now(TZ).isoformat(),
            "channels": channels,
            "error": f"{type(exc).__name__}: {exc}",
            "elapsed_seconds": round(time.perf_counter() - started, 4),
        }


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    cases = {
        "8105_alone": ["tse_8105.tw"],
        "2330_control": ["tse_2330.tw"],
        "mixed": ["tse_8104.tw", "tse_8105.tw", "tse_8110.tw", "tse_2330.tw"],
    }
    s = session()
    results = {}
    try:
        for name, channels in cases.items():
            rows = []
            for _ in range(10):
                rows.append(one(s, channels))
                time.sleep(0.35)
            results[name] = rows
    finally:
        s.close()

    summary = {
        "component": "A4_001V",
        "version": VERSION,
        "checked_at": datetime.now(TZ).isoformat(),
        "cases": {},
    }
    for name, rows in results.items():
        summary["cases"][name] = {
            "runs": len(rows),
            "http_ok": sum(1 for r in rows if r.get("status") == 200),
            "rtcode_0000": sum(1 for r in rows if str(r.get("rtcode")) == "0000"),
            "returned_code_sets": [r.get("returned_codes", []) for r in rows],
            "errors": [r.get("error") for r in rows if r.get("error")],
        }
    summary["8105_return_count"] = sum(
        1 for group in results.values() for row in group if "8105" in row.get("returned_codes", [])
    )
    summary["2330_return_count"] = sum(
        1 for group in results.values() for row in group if "2330" in row.get("returned_codes", [])
    )
    summary["source_anomaly_8105"] = summary["8105_return_count"] == 0 and summary["2330_return_count"] > 0
    (OUT / "raw_probe.json").write_text(json.dumps(results, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUT / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if summary["2330_return_count"] > 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
