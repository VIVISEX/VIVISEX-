from __future__ import annotations

import csv
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

TZ = ZoneInfo("Asia/Taipei")
MIS_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
RUN_MODE = os.getenv("RUN_MODE", "live").strip().lower()
STOCKS_FILE = Path(os.getenv("STOCKS_FILE", "stocks.csv"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output/mis"))
POLL_SECONDS = max(1.0, float(os.getenv("POLL_SECONDS", "5")))
START_TIME = dtime.fromisoformat(os.getenv("START_TIME", "08:30:00"))
PREOPEN_END = dtime.fromisoformat(os.getenv("PREOPEN_END", "09:00:00"))
END_TIME = dtime.fromisoformat(os.getenv("END_TIME", "09:30:00"))
BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "50")))
MAX_RETRIES = max(1, int(os.getenv("MAX_RETRIES", "5")))
REQUEST_TIMEOUT = max(3.0, float(os.getenv("REQUEST_TIMEOUT", "8")))


@dataclass(frozen=True)
class Stock:
    market: str
    code: str
    ex_ch: str


def now_tw() -> datetime:
    return datetime.now(TZ)


def parse_levels(value: Any, cast=float) -> list[Any]:
    if value is None:
        return []
    out: list[Any] = []
    for token in str(value).split("_"):
        token = token.strip()
        if not token:
            continue
        try:
            out.append(cast(token))
        except Exception:
            out.append(None)
    return out[:5]


def load_stocks(path: Path) -> list[Stock]:
    if not path.exists():
        raise FileNotFoundError(f"stocks file not found: {path}")
    stocks: list[Stock] = []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            code = str(row.get("code", "")).strip()
            market = str(row.get("market", "TSE")).strip().upper()
            ex_ch = str(row.get("ex_ch", "")).strip()
            if not code:
                continue
            if not ex_ch:
                prefix = "tse" if market == "TSE" else "otc"
                ex_ch = f"{prefix}_{code}.tw"
            stocks.append(Stock(market=market, code=code, ex_ch=ex_ch))
    if not stocks:
        raise RuntimeError("stocks.csv contains no valid symbols")
    return stocks


def chunks(seq: list[Stock], n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 QTS-A4-LIVE-PREOPEN/1.0",
            "Accept": "application/json,text/plain,*/*",
            "Referer": "https://mis.twse.com.tw/stock/fibest.jsp",
            "Cache-Control": "no-cache",
        }
    )
    return s


def fetch_batch(session: requests.Session, batch: list[Stock]) -> dict[str, Any]:
    params = {
        "ex_ch": "|".join(s.ex_ch for s in batch),
        "json": "1",
        "delay": "0",
        "_": str(int(time.time() * 1000)),
    }
    last: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(MIS_URL, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            obj = r.json()
            if not isinstance(obj, dict):
                raise RuntimeError("MIS response is not an object")
            return obj
        except Exception as exc:
            last = exc
            if attempt < MAX_RETRIES:
                time.sleep(min(4.0, 0.5 * (2 ** (attempt - 1))))
    raise RuntimeError(f"MIS request failed after {MAX_RETRIES} retries: {last}")


def phase_for(t: dtime) -> str:
    if t < START_TIME:
        return "WAIT"
    if t < PREOPEN_END:
        return "PREOPEN"
    if t <= END_TIME:
        return "OPEN_VALIDATION"
    return "DONE"


def normalize_item(item: dict[str, Any], captured_at: datetime, phase: str) -> dict[str, Any]:
    bid_price = parse_levels(item.get("b"), float)
    ask_price = parse_levels(item.get("a"), float)
    bid_volume = parse_levels(item.get("g"), int)
    ask_volume = parse_levels(item.get("f"), int)

    return {
        "captured_at": captured_at.isoformat(timespec="milliseconds"),
        "phase": phase,
        "market": str(item.get("ex", "")),
        "code": str(item.get("c", "")),
        "name": str(item.get("n", "")),
        "market_date": str(item.get("d", "")),
        "market_time": str(item.get("t", "")),
        "simulated_flag": str(item.get("ts", "")),
        "last_price": item.get("z"),
        "single_volume": item.get("tv"),
        "total_volume": item.get("v"),
        "reference_price": item.get("y"),
        "open": item.get("o"),
        "high": item.get("h"),
        "low": item.get("l"),
        "bid_price_1": bid_price[0] if len(bid_price) > 0 else None,
        "bid_price_2": bid_price[1] if len(bid_price) > 1 else None,
        "bid_price_3": bid_price[2] if len(bid_price) > 2 else None,
        "bid_price_4": bid_price[3] if len(bid_price) > 3 else None,
        "bid_price_5": bid_price[4] if len(bid_price) > 4 else None,
        "bid_volume_1": bid_volume[0] if len(bid_volume) > 0 else None,
        "bid_volume_2": bid_volume[1] if len(bid_volume) > 1 else None,
        "bid_volume_3": bid_volume[2] if len(bid_volume) > 2 else None,
        "bid_volume_4": bid_volume[3] if len(bid_volume) > 3 else None,
        "bid_volume_5": bid_volume[4] if len(bid_volume) > 4 else None,
        "ask_price_1": ask_price[0] if len(ask_price) > 0 else None,
        "ask_price_2": ask_price[1] if len(ask_price) > 1 else None,
        "ask_price_3": ask_price[2] if len(ask_price) > 2 else None,
        "ask_price_4": ask_price[3] if len(ask_price) > 3 else None,
        "ask_price_5": ask_price[4] if len(ask_price) > 4 else None,
        "ask_volume_1": ask_volume[0] if len(ask_volume) > 0 else None,
        "ask_volume_2": ask_volume[1] if len(ask_volume) > 1 else None,
        "ask_volume_3": ask_volume[2] if len(ask_volume) > 2 else None,
        "ask_volume_4": ask_volume[3] if len(ask_volume) > 3 else None,
        "ask_volume_5": ask_volume[4] if len(ask_volume) > 4 else None,
    }


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def run_env_test(stocks: list[Stock]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    session = make_session()
    started = now_tw()
    errors: list[str] = []
    items: list[dict[str, Any]] = []
    raws: list[dict[str, Any]] = []

    for batch in chunks(stocks, BATCH_SIZE):
        try:
            raw = fetch_batch(session, batch)
            raws.append(raw)
            for item in raw.get("msgArray", []) or []:
                items.append(normalize_item(item, now_tw(), "ENV_TEST"))
        except Exception as exc:
            errors.append(str(exc))

    report = {
        "mode": "env_test",
        "started_at": started.isoformat(),
        "finished_at": now_tw().isoformat(),
        "requested_symbols": len(stocks),
        "normalized_rows": len(items),
        "errors": errors,
        "pass": len(items) > 0 and not errors,
    }
    atomic_write_json(OUTPUT_DIR / "env_test_report.json", report)
    atomic_write_json(OUTPUT_DIR / "env_test_raw.json", raws)
    if items:
        pd.DataFrame(items).to_csv(OUTPUT_DIR / "env_test_normalized.csv", index=False, encoding="utf-8-sig")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 2


def run_live(stocks: list[Stock]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    day = now_tw().strftime("%Y-%m-%d")
    raw_path = OUTPUT_DIR / f"raw_{day}.ndjson"
    norm_path = OUTPUT_DIR / f"normalized_{day}.parquet"
    csv_path = OUTPUT_DIR / f"normalized_{day}.csv"
    report_path = OUTPUT_DIR / f"report_{day}.json"

    scheduled_start = datetime.combine(now_tw().date(), START_TIME, TZ)
    actual_process_start = now_tw()
    schedule_delay_seconds = max(0.0, (actual_process_start - scheduled_start.replace(hour=8, minute=25, second=0)).total_seconds())

    while phase_for(now_tw().time()) == "WAIT":
        remaining = (scheduled_start - now_tw()).total_seconds()
        time.sleep(max(0.2, min(5.0, remaining)))

    if phase_for(now_tw().time()) == "DONE":
        report = {
            "mode": "live",
            "pass": False,
            "reason": "started_after_end_time",
            "actual_process_start": actual_process_start.isoformat(),
        }
        atomic_write_json(report_path, report)
        print(json.dumps(report, ensure_ascii=False))
        return 3

    session = make_session()
    normalized: list[dict[str, Any]] = []
    request_count = 0
    error_count = 0
    empty_count = 0
    preopen_rows = 0
    open_rows = 0
    first_capture: str | None = None
    last_capture: str | None = None
    timestamps: list[datetime] = []

    next_poll = time.monotonic()
    with raw_path.open("a", encoding="utf-8") as raw_file:
        while True:
            current = now_tw()
            phase = phase_for(current.time())
            if phase == "DONE":
                break

            if time.monotonic() < next_poll:
                time.sleep(min(0.2, next_poll - time.monotonic()))
                continue

            captured = now_tw()
            cycle_had_rows = False
            for batch in chunks(stocks, BATCH_SIZE):
                request_count += 1
                try:
                    raw = fetch_batch(session, batch)
                    envelope = {
                        "captured_at": captured.isoformat(timespec="milliseconds"),
                        "phase": phase,
                        "request_symbols": [s.ex_ch for s in batch],
                        "raw": raw,
                    }
                    raw_file.write(json.dumps(envelope, ensure_ascii=False) + "\n")
                    raw_file.flush()
                    items = raw.get("msgArray", []) or []
                    if not items:
                        empty_count += 1
                    for item in items:
                        row = normalize_item(item, captured, phase)
                        normalized.append(row)
                        cycle_had_rows = True
                        if phase == "PREOPEN":
                            preopen_rows += 1
                        elif phase == "OPEN_VALIDATION":
                            open_rows += 1
                except Exception as exc:
                    error_count += 1
                    raw_file.write(json.dumps({"captured_at": captured.isoformat(), "phase": phase, "error": str(exc)}, ensure_ascii=False) + "\n")
                    raw_file.flush()

            if cycle_had_rows:
                timestamps.append(captured)
                if first_capture is None:
                    first_capture = captured.isoformat()
                last_capture = captured.isoformat()

            next_poll += POLL_SECONDS
            if next_poll < time.monotonic() - POLL_SECONDS:
                next_poll = time.monotonic() + POLL_SECONDS

    if normalized:
        df = pd.DataFrame(normalized)
        df.to_parquet(norm_path, index=False)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    gaps = []
    if len(timestamps) >= 2:
        gaps = [(b - a).total_seconds() for a, b in zip(timestamps, timestamps[1:])]
    expected_cycles = int(((datetime.combine(now_tw().date(), END_TIME, TZ) - datetime.combine(now_tw().date(), START_TIME, TZ)).total_seconds()) / POLL_SECONDS)
    actual_cycles = len(timestamps)
    missing_cycles = max(0, expected_cycles - actual_cycles)
    missing_rate = (missing_cycles / expected_cycles) if expected_cycles else 0.0

    report = {
        "mode": "live",
        "date": day,
        "actual_process_start": actual_process_start.isoformat(),
        "first_capture": first_capture,
        "last_capture": last_capture,
        "schedule_delay_seconds_from_0825": schedule_delay_seconds,
        "poll_seconds": POLL_SECONDS,
        "requested_symbols": len(stocks),
        "requests": request_count,
        "normalized_rows": len(normalized),
        "preopen_rows": preopen_rows,
        "open_validation_rows": open_rows,
        "error_count": error_count,
        "empty_response_count": empty_count,
        "expected_cycles_0830_0930": expected_cycles,
        "actual_cycles_with_rows": actual_cycles,
        "missing_cycles": missing_cycles,
        "missing_rate": missing_rate,
        "max_gap_seconds": max(gaps) if gaps else None,
        "pass_candidate": preopen_rows > 0 and open_rows > 0 and error_count == 0,
        "raw_file": str(raw_path),
        "normalized_parquet": str(norm_path),
        "normalized_csv": str(csv_path),
    }
    atomic_write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass_candidate"] else 4


def main() -> int:
    stocks = load_stocks(STOCKS_FILE)
    if RUN_MODE == "env_test":
        return run_env_test(stocks)
    if RUN_MODE != "live":
        raise ValueError(f"unsupported RUN_MODE={RUN_MODE}")
    return run_live(stocks)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            OUTPUT_DIR / "fatal_error.json",
            {"at": now_tw().isoformat(), "error": repr(exc), "run_mode": RUN_MODE},
        )
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(99)
