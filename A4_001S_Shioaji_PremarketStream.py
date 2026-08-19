from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_001S_STREAM_V1.3.0"
UNIVERSE = Path(os.getenv("A4_UNIVERSE_FILE", "output/universe/stocks.csv"))
OUT = Path(os.getenv("SHIOAJI_OUTPUT_DIR", "output/shioaji"))
GAP_SEC = float(os.getenv("SJ_SOURCE_GAP_SECONDS", "30"))
CACHE_FLUSH_SEC = min(2.0, max(0.10, float(os.getenv("SJ_CACHE_FLUSH_SECONDS", "0.50"))))
SUBS_PER_CONNECTION = min(200, max(1, int(os.getenv("SJ_SUBS_PER_CONNECTION", "200"))))
MAX_CONNECTIONS = min(5, max(1, int(os.getenv("SJ_MAX_CONNECTIONS", "5"))))
MAX_SYMBOLS = SUBS_PER_CONNECTION * MAX_CONNECTIONS
LOGIN_RETRIES = min(3, max(1, int(os.getenv("SJ_LOGIN_RETRIES", "3"))))
RECEIVE_WINDOW_MS = max(30000, int(os.getenv("SJ_RECEIVE_WINDOW_MS", "60000")))


def now() -> datetime:
    return datetime.now(TZ)


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def load_symbols() -> list[str]:
    if not UNIVERSE.exists():
        raise RuntimeError(f"universe missing: {UNIVERSE}")
    out: list[str] = []
    seen: set[str] = set()
    with UNIVERSE.open(encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            c = str(r.get("code", "")).strip().upper()
            if not c or c in seen:
                continue
            seen.add(c)
            out.append(c)
    if not out:
        raise RuntimeError("empty universe")
    if len(out) > MAX_SYMBOLS:
        raise RuntimeError(f"UNIVERSE_CAPACITY_EXCEEDED requested={len(out)} verified_capacity={MAX_SYMBOLS}")
    return out


def norm(exchange: Any, payload: Any) -> dict[str, Any]:
    def g(name: str, default: Any = None) -> Any:
        return payload.get(name, default) if isinstance(payload, dict) else getattr(payload, name, default)

    def scalar(name: str) -> Any:
        v = g(name)
        return str(v) if v is not None else None

    def l5(name: str) -> list[Any]:
        values = list(g(name, []) or [])[:5]
        return [str(x) if x is not None else None for x in values] + [None] * (5 - len(values))

    rec = {
        "source": "SHIOAJI",
        "captured_at": now().isoformat(timespec="milliseconds"),
        "exchange": str(getattr(exchange, "value", exchange)),
        "code": str(g("code", "")),
        "market_date": str(g("date", "")),
        "market_time": str(g("time", "")),
        "simtrade": bool(g("simtrade", False)),
        "suspend": bool(g("suspend", False)),
        "open": scalar("open"),
        "close": scalar("close"),
        "high": scalar("high"),
        "low": scalar("low"),
        "volume": g("volume"),
        "total_volume": g("total_volume"),
        "price_chg": scalar("price_chg"),
        "pct_chg": g("pct_chg"),
    }
    for key in ("bid_price", "bid_volume", "ask_price", "ask_volume", "diff_bid_vol", "diff_ask_vol"):
        vals = l5(key)
        for i, value in enumerate(vals, 1):
            rec[f"{key}_{i}"] = value
    return rec


def resolve_contract(api: Any, code: str) -> Any:
    contract = api.Contracts.Stocks.get(code)
    if contract is None:
        raise RuntimeError(f"SHIOAJI_CONTRACT_NOT_FOUND:{code}")
    return contract


def login_with_retry(sj: Any, key: str, secret: str, slot: int) -> Any:
    errors: list[str] = []
    for attempt in range(1, LOGIN_RETRIES + 1):
        api = sj.Shioaji(simulation=False)
        try:
            api.login(
                api_key=key,
                secret_key=secret,
                subscribe_trade=False,
                receive_window=RECEIVE_WINDOW_MS,
            )
            return api
        except Exception as exc:
            errors.append(f"attempt={attempt}:{type(exc).__name__}:{exc}")
            try:
                api.logout()
            except Exception:
                pass
            if attempt < LOGIN_RETRIES:
                time.sleep(min(4.0, 0.75 * (2 ** (attempt - 1)) + slot * 0.15))
    raise RuntimeError(f"SHIOAJI_LOGIN_FAILED slot={slot} errors={' | '.join(errors)}")


def selftest() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    sample = {
        "code": "2330",
        "date": "2026-08-19",
        "time": "08:45:00",
        "open": 100,
        "close": 101,
        "high": 102,
        "low": 99,
        "volume": 10,
        "total_volume": 500,
        "price_chg": 1,
        "pct_chg": 100,
        "bid_price": [1, 0.9, 0.8, 0.7, 0.6],
        "bid_volume": [1, 2, 3, 4, 5],
        "ask_price": [1.1, 1.2, 1.3, 1.4, 1.5],
        "ask_volume": [6, 7, 8, 9, 10],
        "diff_bid_vol": [1, 1, 1, 1, 1],
        "diff_ask_vol": [-1, -1, -1, -1, -1],
        "simtrade": True,
    }
    r = norm("TSE", sample)
    fake_symbols = [str(i) for i in range(MAX_SYMBOLS)]
    chunks = [fake_symbols[i:i + SUBS_PER_CONNECTION] for i in range(0, len(fake_symbols), SUBS_PER_CONNECTION)]
    checks = {
        "simtrade": r["simtrade"] is True,
        "close_preserved": r["close"] == "101",
        "five_bid": r["bid_price_5"] == "0.6",
        "five_ask": r["ask_volume_5"] == "10",
        "diff": r["diff_ask_vol_1"] == "-1",
        "source_labeled": r["source"] == "SHIOAJI",
        "subscription_cap_per_connection": SUBS_PER_CONNECTION <= 200,
        "connection_cap": MAX_CONNECTIONS <= 5,
        "shard_capacity": len(chunks) <= 5 and all(len(c) <= 200 for c in chunks),
        "receive_window": RECEIVE_WINDOW_MS >= 30000,
        "bounded_login_retry": 1 <= LOGIN_RETRIES <= 3,
        "cache_flush_bounded": 0.10 <= CACHE_FLUSH_SEC <= 2.0,
    }
    rep = {
        "component": "A4_001S",
        "version": VERSION,
        "mode": "selftest",
        "configured_capacity": MAX_SYMBOLS,
        "checks": checks,
        "sample": r,
        "pass": all(checks.values()),
    }
    atomic_json(OUT / "selftest.json", rep)
    print(json.dumps(rep, ensure_ascii=False))
    return 0 if rep["pass"] else 2


def live(seconds: int) -> int:
    try:
        import shioaji as sj
    except Exception as exc:
        raise RuntimeError(f"shioaji import failed: {exc}") from exc

    key = os.getenv("SJ_API_KEY", "").strip()
    secret = os.getenv("SJ_SEC_KEY", "").strip()
    if not key or not secret:
        raise RuntimeError("SJ_API_KEY/SJ_SEC_KEY missing")

    symbols = load_symbols()
    chunks = [symbols[i:i + SUBS_PER_CONNECTION] for i in range(0, len(symbols), SUBS_PER_CONNECTION)]
    if len(chunks) > MAX_CONNECTIONS:
        raise RuntimeError(f"SHARD_COUNT_EXCEEDED shards={len(chunks)} max={MAX_CONNECTIONS}")

    OUT.mkdir(parents=True, exist_ok=True)
    stamp = now().strftime("%Y%m%d_%H%M%S")
    raw = OUT / f"shioaji_{stamp}.ndjson"
    report = OUT / f"shioaji_{stamp}_report.json"
    latest_path = OUT / "latest_quotes.json"

    lock = threading.Lock()
    last_event = time.monotonic()
    seen: set[str] = set()
    counts = 0
    sim = 0
    five = 0
    unresolved: list[dict[str, Any]] = []
    subscribed_codes: set[str] = set()
    latest: dict[str, dict[str, Any]] = {}
    cache_dirty = False
    apis: list[Any] = []
    fh = raw.open("a", encoding="utf-8")

    def persist(exchange: Any, payload: Any) -> None:
        nonlocal last_event, counts, sim, five, cache_dirty
        rec = norm(exchange, payload)
        code = rec["code"]
        with lock:
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
            fh.flush()
            last_event = time.monotonic()
            counts += 1
            if rec["simtrade"]:
                sim += 1
            if code:
                seen.add(code)
                latest[code] = rec
                cache_dirty = True
            if all(rec.get(f"{side}_{kind}_{i}") is not None for side in ("bid", "ask") for kind in ("price", "volume") for i in range(1, 6)):
                five += 1

    started = now().isoformat()
    gap = False
    try:
        for slot, chunk in enumerate(chunks, start=1):
            api = login_with_retry(sj, key, secret, slot)
            apis.append(api)
            api.set_on_quote_stk_v1_callback(persist)
            for code in chunk:
                try:
                    contract = resolve_contract(api, code)
                    api.quote.subscribe(
                        contract,
                        quote_type=sj.constant.QuoteType.Quote,
                        version=sj.constant.QuoteVersion.v1,
                    )
                    subscribed_codes.add(code)
                except Exception as exc:
                    unresolved.append({"code": code, "slot": slot, "error": f"{type(exc).__name__}: {exc}"})
            time.sleep(0.25)

        if not subscribed_codes:
            raise RuntimeError("no Shioaji contracts subscribed")

        deadline = time.monotonic() + max(1, seconds)
        next_cache_flush = time.monotonic()
        while time.monotonic() < deadline:
            if counts > 0 and time.monotonic() - last_event > GAP_SEC:
                gap = True
                break
            if time.monotonic() >= next_cache_flush:
                with lock:
                    if cache_dirty:
                        snapshot = {
                            "component": "A4_001S",
                            "version": VERSION,
                            "updated_at": now().isoformat(timespec="milliseconds"),
                            "symbols": dict(latest),
                        }
                        cache_dirty = False
                    else:
                        snapshot = None
                if snapshot is not None:
                    atomic_json(latest_path, snapshot)
                next_cache_flush = time.monotonic() + CACHE_FLUSH_SEC
            time.sleep(0.05)
    finally:
        with lock:
            if latest:
                atomic_json(latest_path, {
                    "component": "A4_001S",
                    "version": VERSION,
                    "updated_at": now().isoformat(timespec="milliseconds"),
                    "symbols": dict(latest),
                })
        for api in reversed(apis):
            try:
                api.logout()
            except Exception:
                pass
        fh.close()

    coverage = len(seen) / len(subscribed_codes) if subscribed_codes else 0.0
    rep = {
        "component": "A4_001S",
        "version": VERSION,
        "mode": "live",
        "quote_type": "Quote",
        "started_at": started,
        "finished_at": now().isoformat(),
        "requested_symbols": len(symbols),
        "configured_capacity": MAX_SYMBOLS,
        "connections_used": len(apis),
        "subscribed_symbols": len(subscribed_codes),
        "unresolved_symbols": unresolved,
        "events": counts,
        "seen_symbols": len(seen),
        "coverage_of_subscribed": coverage,
        "simtrade_events": sim,
        "five_level_events": five,
        "source_gap": gap,
        "latest_cache": str(latest_path),
        "pass": bool(subscribed_codes) and counts > 0 and five > 0 and not gap and not unresolved,
    }
    atomic_json(report, rep)
    print(json.dumps(rep, ensure_ascii=False))
    return 0 if rep["pass"] else 3


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--seconds", type=int, default=30)
    a = p.parse_args()
    return selftest() if a.self_test else live(a.seconds)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        OUT.mkdir(parents=True, exist_ok=True)
        err = {"component": "A4_001S", "version": VERSION, "pass": False, "error": f"{type(exc).__name__}: {exc}"}
        atomic_json(OUT / "failure.json", err)
        print(json.dumps(err, ensure_ascii=False), file=sys.stderr)
        sys.exit(99)
