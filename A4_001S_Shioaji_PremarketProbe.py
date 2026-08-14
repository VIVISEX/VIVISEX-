from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
STOCKS_FILE = Path(os.getenv("STOCKS_FILE", "stocks.csv"))
OUTPUT_DIR = Path(os.getenv("SHIOAJI_OUTPUT_DIR", "output/shioaji"))


@dataclass(frozen=True)
class Symbol:
    market: str
    code: str


def now_tw() -> datetime:
    return datetime.now(TZ)


def scalar(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, tuple):
        return list(value)
    return value


def list5(value: Any) -> list[Any]:
    values = list(value or [])[:5]
    return [scalar(v) for v in values] + [None] * (5 - len(values))


def payload_get(payload: Any, name: str, default: Any = None) -> Any:
    if isinstance(payload, dict):
        return payload.get(name, default)
    return getattr(payload, name, default)


def normalize_event(event_type: str, exchange: Any, payload: Any) -> dict[str, Any]:
    bid_price = list5(payload_get(payload, "bid_price", []))
    bid_volume = list5(payload_get(payload, "bid_volume", []))
    ask_price = list5(payload_get(payload, "ask_price", []))
    ask_volume = list5(payload_get(payload, "ask_volume", []))
    diff_bid_vol = list5(payload_get(payload, "diff_bid_vol", []))
    diff_ask_vol = list5(payload_get(payload, "diff_ask_vol", []))

    record: dict[str, Any] = {
        "source": "SHIOAJI",
        "captured_at": now_tw().isoformat(timespec="milliseconds"),
        "event_type": event_type,
        "exchange": str(getattr(exchange, "value", exchange)),
        "code": str(payload_get(payload, "code", "")),
        "market_date": scalar(payload_get(payload, "date")),
        "market_time": scalar(payload_get(payload, "time")),
        "simtrade": bool(payload_get(payload, "simtrade", False)),
        "suspend": bool(payload_get(payload, "suspend", False)),
        "close": scalar(payload_get(payload, "close")),
        "volume": scalar(payload_get(payload, "volume")),
        "total_volume": scalar(payload_get(payload, "total_volume")),
        "price_chg": scalar(payload_get(payload, "price_chg")),
        "pct_chg": scalar(payload_get(payload, "pct_chg")),
    }
    for index in range(5):
        level = index + 1
        record[f"bid_price_{level}"] = bid_price[index]
        record[f"bid_volume_{level}"] = bid_volume[index]
        record[f"ask_price_{level}"] = ask_price[index]
        record[f"ask_volume_{level}"] = ask_volume[index]
        record[f"diff_bid_vol_{level}"] = diff_bid_vol[index]
        record[f"diff_ask_vol_{level}"] = diff_ask_vol[index]
    return record


def load_symbols(path: Path) -> list[Symbol]:
    if not path.exists():
        raise FileNotFoundError(path)
    result: list[Symbol] = []
    seen: set[str] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            code = str(row.get("code", "")).strip()
            market = str(row.get("market", "TSE")).strip().upper()
            if not code or code in seen:
                continue
            if market not in {"TSE", "OTC"}:
                raise ValueError(f"unsupported market={market} code={code}")
            seen.add(code)
            result.append(Symbol(market=market, code=code))
    if not result:
        raise RuntimeError("no symbols")
    return result


def five_level_complete(record: dict[str, Any]) -> bool:
    keys = []
    for side in ("bid", "ask"):
        for kind in ("price", "volume"):
            keys.extend(f"{side}_{kind}_{i}" for i in range(1, 6))
    return all(record.get(key) is not None for key in keys)


def self_test() -> int:
    symbols = load_symbols(STOCKS_FILE)
    synthetic = {
        "code": "2330",
        "date": (2026, 8, 17),
        "time": (8, 45, 0, 123456),
        "close": Decimal("1234.5"),
        "volume": 100,
        "total_volume": 0,
        "price_chg": Decimal("4.5"),
        "pct_chg": 37,
        "bid_price": [Decimal("1234"), Decimal("1233.5"), Decimal("1233"), Decimal("1232.5"), Decimal("1232")],
        "bid_volume": [10, 20, 30, 40, 50],
        "diff_bid_vol": [1, -2, 3, 0, 5],
        "ask_price": [Decimal("1235"), Decimal("1235.5"), Decimal("1236"), Decimal("1236.5"), Decimal("1237")],
        "ask_volume": [11, 21, 31, 41, 51],
        "diff_ask_vol": [-1, 2, -3, 0, -5],
        "simtrade": True,
        "suspend": False,
    }
    record = normalize_event("quote_stk", "TSE", synthetic)
    checks = {
        "symbols_loaded": len(symbols) == 5,
        "five_level_complete": five_level_complete(record),
        "simtrade_preserved": record["simtrade"] is True,
        "diff_volume_preserved": record["diff_bid_vol_2"] == -2 and record["diff_ask_vol_5"] == -5,
        "decimal_normalized": record["close"] == 1234.5,
        "source_labeled": record["source"] == "SHIOAJI",
    }
    passed = all(checks.values())
    report = {
        "component": "A4_001S",
        "mode": "self_test",
        "checked_at": now_tw().isoformat(),
        "checks": checks,
        "sample": record,
        "pass": passed,
    }
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "selftest_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(json.dumps(report, ensure_ascii=False))
    return 0 if passed else 2


def live_probe(seconds: int) -> int:
    try:
        import shioaji as sj
    except Exception as exc:
        raise RuntimeError(f"shioaji import failed: {exc}") from exc

    api_key = os.getenv("SJ_API_KEY", "").strip()
    secret_key = os.getenv("SJ_SEC_KEY", "").strip()
    if not api_key or not secret_key:
        raise RuntimeError("SJ_API_KEY and SJ_SEC_KEY are required for live probe")

    symbols = load_symbols(STOCKS_FILE)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = now_tw().strftime("%Y%m%d_%H%M%S")
    raw_path = OUTPUT_DIR / f"shioaji_probe_{stamp}.ndjson"
    report_path = OUTPUT_DIR / f"shioaji_probe_{stamp}_report.json"
    lock = threading.Lock()
    event_counts = {"quote_stk": 0, "bidask_stk": 0}
    simtrade_counts = {"quote_stk": 0, "bidask_stk": 0}
    seen_codes: set[str] = set()
    five_level_count = 0
    started_at = now_tw()

    api = sj.Shioaji(simulation=False)
    api.login(api_key=api_key, secret_key=secret_key, subscribe_trade=False)

    with raw_path.open("a", encoding="utf-8") as raw_file:
        def persist(event_type: str, exchange: Any, payload: Any) -> None:
            nonlocal five_level_count
            record = normalize_event(event_type, exchange, payload)
            with lock:
                raw_file.write(json.dumps(record, ensure_ascii=False) + "\n")
                raw_file.flush()
                event_counts[event_type] += 1
                if record.get("simtrade"):
                    simtrade_counts[event_type] += 1
                if record.get("code"):
                    seen_codes.add(str(record["code"]))
                if five_level_complete(record):
                    five_level_count += 1

        def quote_callback(exchange: Any, quote: Any) -> None:
            persist("quote_stk", exchange, quote)

        def bidask_callback(exchange: Any, bidask: Any) -> None:
            persist("bidask_stk", exchange, bidask)

        api.set_on_quote_stk_v1_callback(quote_callback)
        api.set_on_bidask_stk_v1_callback(bidask_callback)

        contracts = []
        for symbol in symbols:
            contract = api.contracts.get(symbol.code)
            if contract is None:
                raise RuntimeError(f"contract not found: {symbol.code}")
            contracts.append(contract)
            api.subscribe(contract, quote_type=sj.QuoteType.Quote)
            api.subscribe(contract, quote_type=sj.QuoteType.BidAsk)

        try:
            deadline = time.monotonic() + max(1, seconds)
            while time.monotonic() < deadline:
                time.sleep(0.2)
        finally:
            for contract in contracts:
                try:
                    api.unsubscribe(contract, quote_type=sj.QuoteType.Quote)
                except Exception:
                    pass
                try:
                    api.unsubscribe(contract, quote_type=sj.QuoteType.BidAsk)
                except Exception:
                    pass
            try:
                api.logout()
            except Exception:
                pass

    report = {
        "component": "A4_001S",
        "mode": "live_probe",
        "started_at": started_at.isoformat(),
        "finished_at": now_tw().isoformat(),
        "seconds": seconds,
        "symbols": [asdict(symbol) for symbol in symbols],
        "event_counts": event_counts,
        "simtrade_counts": simtrade_counts,
        "seen_codes": sorted(seen_codes),
        "five_level_event_count": five_level_count,
        "preopen_support_observed": (sum(simtrade_counts.values()) > 0),
        "pass": (sum(event_counts.values()) > 0 and five_level_count > 0),
        "note": "A live probe outside 08:30-09:00 validates authentication and streaming only. Premarket support is accepted only when simtrade events are observed during the real preopen window.",
        "raw_file": str(raw_path),
    }
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 3


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["self-test", "live-probe"], default="self-test")
    parser.add_argument("--seconds", type=int, default=30)
    args = parser.parse_args()
    if args.mode == "self-test":
        return self_test()
    return live_probe(args.seconds)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        failure = {
            "component": "A4_001S",
            "at": now_tw().isoformat(),
            "pass": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
        (OUTPUT_DIR / "failure.json").write_text(
            json.dumps(failure, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(json.dumps(failure, ensure_ascii=False), file=sys.stderr)
        sys.exit(99)
