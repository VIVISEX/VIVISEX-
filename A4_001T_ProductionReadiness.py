from __future__ import annotations

import importlib.util
import json
import sys
import time
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent
PROD_PATH = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
FIXTURE_PATH = ROOT / "tests" / "fixtures" / "a4_mis_2026-08-07_5symbols.json"
OUTPUT_DIR = ROOT / "output" / "readiness"
TZ = ZoneInfo("Asia/Taipei")


def load_production_module():
    spec = importlib.util.spec_from_file_location("a4_prod", PROD_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load production module: {PROD_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def five_level_complete(row: dict[str, Any]) -> bool:
    keys: list[str] = []
    for side in ("bid", "ask"):
        for kind in ("price", "volume"):
            keys.extend(f"{side}_{kind}_{i}" for i in range(1, 6))
    return all(row.get(k) is not None for k in keys)


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    started = datetime.now(TZ)
    results: list[dict[str, Any]] = []
    prod = load_production_module()

    def run(name: str, fn: Callable[[], dict[str, Any] | None]) -> None:
        t0 = time.perf_counter()
        try:
            detail = fn() or {}
            results.append(
                {
                    "name": name,
                    "pass": True,
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                    "detail": detail,
                }
            )
        except Exception as exc:
            results.append(
                {
                    "name": name,
                    "pass": False,
                    "elapsed_ms": round((time.perf_counter() - t0) * 1000, 2),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    def production_contract() -> dict[str, Any]:
        require(PROD_PATH.exists(), "production scraper missing")
        require(prod.MIS_URL == "https://mis.twse.com.tw/stock/api/getStockInfo.jsp", "unexpected MIS endpoint")
        stocks = prod.load_stocks(ROOT / "stocks.csv")
        require(len(stocks) == 5, f"expected 5 production symbols, got {len(stocks)}")
        require(len({s.code for s in stocks}) == len(stocks), "duplicate stock code")
        return {"symbols": [s.code for s in stocks], "endpoint": prod.MIS_URL}

    def state_machine() -> dict[str, Any]:
        cases = {
            dtime(8, 29, 59): "WAIT",
            dtime(8, 30, 0): "PREOPEN",
            dtime(8, 59, 59): "PREOPEN",
            dtime(9, 0, 0): "OPEN_VALIDATION",
            dtime(9, 29, 59): "OPEN_VALIDATION",
            dtime(9, 30, 0): "DONE",
        }
        observed = {t.isoformat(): prod.phase_for(t) for t in cases}
        for t, expected in cases.items():
            require(prod.phase_for(t) == expected, f"phase {t} expected {expected}, got {prod.phase_for(t)}")
        return {"observed": observed}

    def historical_replay() -> dict[str, Any]:
        fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
        items = fixture.get("msgArray") or []
        require(len(items) == 5, f"fixture expected 5 rows, got {len(items)}")
        captured = datetime(2026, 8, 9, 22, 17, 48, tzinfo=TZ)
        rows = [prod.normalize_item(item, captured, "REPLAY") for item in items]
        expected = {"2330", "2317", "2454", "2308", "2382"}
        returned = {str(row.get("code")) for row in rows}
        require(returned == expected, f"fixture symbol mismatch: {sorted(returned)}")
        require(all(str(row.get("market_date")) == "20260807" for row in rows), "fixture market date mismatch")
        incomplete = [str(row.get("code")) for row in rows if not five_level_complete(row)]
        require(not incomplete, f"fixture five-level incomplete: {incomplete}")
        return {
            "rows": len(rows),
            "symbols": sorted(returned),
            "market_date": "20260807",
            "five_level_complete": True,
        }

    def live_mis_probe() -> dict[str, Any]:
        stocks = prod.load_stocks(ROOT / "stocks.csv")
        session = prod.make_session()
        t0 = time.perf_counter()
        raw = prod.fetch_batch(session, stocks)
        latency_ms = round((time.perf_counter() - t0) * 1000, 2)
        require(isinstance(raw, dict), "live MIS response is not object")
        require(str(raw.get("rtcode", "")) == "0000", f"live MIS rtcode={raw.get('rtcode')}")
        items = raw.get("msgArray") or []
        require(items, "live MIS msgArray empty")
        captured = datetime.now(TZ)
        rows = [prod.normalize_item(item, captured, "READINESS_LIVE_PROBE") for item in items]
        expected = {s.code for s in stocks}
        returned = {str(row.get("code")) for row in rows if row.get("code")}
        require(returned == expected, f"live symbol mismatch expected={sorted(expected)} returned={sorted(returned)}")
        incomplete = [str(row.get("code")) for row in rows if not five_level_complete(row)]
        require(not incomplete, f"live five-level incomplete: {incomplete}")
        market_dates = sorted({str(row.get("market_date")) for row in rows if row.get("market_date")})
        require(market_dates, "live market_date missing")
        today_compact = captured.strftime("%Y%m%d")
        require(all(d <= today_compact for d in market_dates), f"future market date returned: {market_dates}")
        query_time = raw.get("queryTime") or {}
        return {
            "latency_ms": latency_ms,
            "rows": len(rows),
            "symbols": sorted(returned),
            "market_dates": market_dates,
            "query_sys_date": query_time.get("sysDate"),
            "query_sys_time": query_time.get("sysTime"),
            "five_level_complete": True,
        }

    run("production_contract", production_contract)
    run("phase_state_machine", state_machine)
    run("historical_replay_2026_08_07", historical_replay)
    run("live_twse_mis_network_probe", live_mis_probe)

    passed = all(item.get("pass") is True for item in results)
    report = {
        "component": "A4_001T",
        "purpose": "Production readiness: production parser replay + real GitHub Runner to TWSE MIS connectivity",
        "started_at": started.isoformat(),
        "finished_at": datetime.now(TZ).isoformat(),
        "pass": passed,
        "tests": results,
        "scope_note": "This test validates runner, dependencies, endpoint reachability, five-level parsing and historical replay. It does not replace the real 08:30-09:00 pre-open production acceptance test.",
    }
    report_path = OUTPUT_DIR / "readiness_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
