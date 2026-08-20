from __future__ import annotations

import csv
import json
import math
import os
import queue
import random
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

VERSION = "1.5.0"
TZ = ZoneInfo("Asia/Taipei")

MIS_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
MIS_HOME_URL = "https://mis.twse.com.tw/stock/index.jsp"
MIS_FIBEST_URL = "https://mis.twse.com.tw/stock/fibest.jsp"

RUN_MODE = os.getenv("RUN_MODE", "live").strip().lower()
STOCKS_FILE = Path(os.getenv("STOCKS_FILE", "stocks.csv"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output/mis"))

POLL_SECONDS = max(1.0, float(os.getenv("POLL_SECONDS", "5")))
ARM_TIME = dtime.fromisoformat(os.getenv("ARM_TIME", "08:25:00"))
START_TIME = dtime.fromisoformat(os.getenv("START_TIME", "08:30:00"))
PREOPEN_END = dtime.fromisoformat(os.getenv("PREOPEN_END", "09:00:00"))
END_TIME = dtime.fromisoformat(os.getenv("END_TIME", "09:30:00"))
STARTUP_DEADLINE = dtime.fromisoformat(os.getenv("STARTUP_DEADLINE", "08:29:45"))
FIRST_PREOPEN_DEADLINE = dtime.fromisoformat(os.getenv("FIRST_PREOPEN_DEADLINE", "08:30:20"))
LAST_PREOPEN_EARLIEST = dtime.fromisoformat(os.getenv("LAST_PREOPEN_EARLIEST", "08:59:40"))

BATCH_SIZE = max(50, int(os.getenv("BATCH_SIZE", "60")))
PRIMARY_WORKERS = max(1, int(os.getenv("PRIMARY_WORKERS", "3")))
RETRY_BATCH_SIZE = max(24, int(os.getenv("RETRY_BATCH_SIZE", "30")))
RETRY_WORKERS = max(1, int(os.getenv("RETRY_WORKERS", "2")))
SINGLE_FALLBACK_LIMIT = max(0, int(os.getenv("SINGLE_FALLBACK_LIMIT", "4")))
SESSION_POOL_SIZE = max(1, int(os.getenv("SESSION_POOL_SIZE", "3")))

PRIMARY_TIMEOUT = max(1.80, float(os.getenv("PRIMARY_TIMEOUT", "2.20")))
RETRY_TIMEOUT = max(1.20, float(os.getenv("RETRY_TIMEOUT", "1.80")))
SINGLE_TIMEOUT = max(1.00, float(os.getenv("SINGLE_TIMEOUT", "1.40")))
CYCLE_BUDGET_SECONDS = max(1.0, float(os.getenv("CYCLE_BUDGET_SECONDS", "4.60")))
CYCLE_GUARD_SECONDS = max(0.05, float(os.getenv("CYCLE_GUARD_SECONDS", "0.15")))
WARMUP_TIMEOUT = max(0.5, float(os.getenv("WARMUP_TIMEOUT", "3.0")))
WARMUP_ATTEMPTS = max(1, int(os.getenv("WARMUP_ATTEMPTS", "3")))

SOURCE_GAP_CONSECUTIVE_CYCLES = max(1, int(os.getenv("SOURCE_GAP_CONSECUTIVE_CYCLES", "3")))
MAX_REQUEST_ERROR_RATE = min(1.0, max(0.0, float(os.getenv("MAX_REQUEST_ERROR_RATE", "0.02"))))
MIN_PREOPEN_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_PREOPEN_COVERAGE", "0.98"))))
MIN_OPEN_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_OPEN_COVERAGE", "0.95"))))
MIN_SYMBOL_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_SYMBOL_COVERAGE", "0.99"))))
MAX_ALLOWED_GAP_SECONDS = max(POLL_SECONDS, float(os.getenv("MAX_ALLOWED_GAP_SECONDS", "15")))
RETRY_TRIGGER_COVERAGE = min(1.0, max(0.0, float(os.getenv("RETRY_TRIGGER_COVERAGE", "0.80"))))

BACKOFF_BASE_SECONDS = max(0.2, float(os.getenv("BACKOFF_BASE_SECONDS", "0.75")))
BACKOFF_CAP_SECONDS = max(BACKOFF_BASE_SECONDS, float(os.getenv("BACKOFF_CAP_SECONDS", "3.0")))

RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}

_SESSION_POOL: queue.LifoQueue[requests.Session] = queue.LifoQueue()
_BACKOFF_LOCK = threading.Lock()
_BACKOFF_UNTIL = 0.0
_BACKOFF_LEVEL = 0


@dataclass(frozen=True)
class Stock:
    market: str
    code: str
    ex_ch: str


@dataclass
class FetchResult:
    requested: list[Stock]
    items: list[dict[str, Any]]
    raw: dict[str, Any] | None
    ok: bool
    error: str | None
    error_type: str | None
    http_status: int | None
    elapsed_seconds: float
    stage: str


def now_tw() -> datetime:
    return datetime.now(TZ)


def combine_today(t: dtime, base: datetime | None = None) -> datetime:
    base = base or now_tw()
    return datetime.combine(base.date(), t, TZ)


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


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
    seen: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "code" not in reader.fieldnames:
            raise ValueError("stocks.csv must contain code column")
        for row in reader:
            code = str(row.get("code", "")).strip()
            market = str(row.get("market", "TSE")).strip().upper()
            ex_ch = str(row.get("ex_ch", "")).strip()
            if not code:
                continue
            if market not in {"TSE", "OTC"}:
                raise ValueError(f"unsupported market={market} for code={code}")
            key = (market, code)
            if key in seen:
                continue
            seen.add(key)
            if not ex_ch:
                prefix = "tse" if market == "TSE" else "otc"
                ex_ch = f"{prefix}_{code}.tw"
            stocks.append(Stock(market=market, code=code, ex_ch=ex_ch))

    if not stocks:
        raise RuntimeError("stocks.csv contains no valid symbols")
    return stocks


def chunks(seq: list[Stock], n: int) -> list[list[Stock]]:
    return [seq[i : i + n] for i in range(0, len(seq), n)]


def make_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=2,
        pool_maxsize=2,
        max_retries=0,
        pool_block=True,
    )
    s.mount("https://", adapter)
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36 QTS-A4/1.5",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
            "Referer": MIS_FIBEST_URL,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
    )
    return s


def classify_exception(exc: Exception) -> tuple[str, int | None]:
    if isinstance(exc, requests.Timeout):
        return "TIMEOUT", None
    if isinstance(exc, requests.ConnectionError):
        return "CONNECTION_ERROR", None
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return f"HTTP_{status}" if status else "HTTP_ERROR", status
    if isinstance(exc, json.JSONDecodeError):
        return "JSON_DECODE_ERROR", None
    msg = str(exc)
    if "rtcode=" in msg:
        return "MIS_RTCODE", None
    if "msgArray" in msg:
        return "MIS_SCHEMA_ERROR", None
    return type(exc).__name__.upper(), None


def wait_for_global_backoff(deadline_monotonic: float) -> bool:
    global _BACKOFF_UNTIL
    while True:
        with _BACKOFF_LOCK:
            remaining = _BACKOFF_UNTIL - time.monotonic()
        if remaining <= 0:
            return True
        if time.monotonic() + remaining >= deadline_monotonic - CYCLE_GUARD_SECONDS:
            return False
        time.sleep(min(remaining, 0.25))


def register_backoff(error_type: str | None) -> None:
    global _BACKOFF_UNTIL, _BACKOFF_LEVEL
    if not error_type:
        return
    severe = error_type in {"HTTP_429", "HTTP_503", "HTTP_502", "CONNECTION_ERROR", "TIMEOUT"}
    with _BACKOFF_LOCK:
        if severe:
            _BACKOFF_LEVEL = min(_BACKOFF_LEVEL + 1, 4)
            delay = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (_BACKOFF_LEVEL - 1)))
            delay += random.uniform(0.0, min(0.25, delay * 0.2))
            _BACKOFF_UNTIL = max(_BACKOFF_UNTIL, time.monotonic() + delay)
        else:
            _BACKOFF_LEVEL = max(0, _BACKOFF_LEVEL - 1)


def register_success() -> None:
    global _BACKOFF_LEVEL
    with _BACKOFF_LOCK:
        _BACKOFF_LEVEL = max(0, _BACKOFF_LEVEL - 1)


def warm_session(session: requests.Session) -> dict[str, Any]:
    errors: list[str] = []
    for attempt in range(1, WARMUP_ATTEMPTS + 1):
        for url in (MIS_HOME_URL, MIS_FIBEST_URL):
            try:
                r = session.get(url, timeout=WARMUP_TIMEOUT, allow_redirects=True)
                if 200 <= r.status_code < 400:
                    return {
                        "ok": True,
                        "attempt": attempt,
                        "status": r.status_code,
                        "url": url,
                        "cookie_count": len(session.cookies),
                    }
                errors.append(f"{url}:HTTP_{r.status_code}")
            except Exception as exc:
                et, _ = classify_exception(exc)
                errors.append(f"{url}:{et}")
        time.sleep(min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * attempt))
    return {"ok": False, "errors": errors[-6:], "cookie_count": len(session.cookies)}


def init_session_pool() -> list[dict[str, Any]]:
    while True:
        try:
            old = _SESSION_POOL.get_nowait()
            old.close()
        except queue.Empty:
            break

    results: list[dict[str, Any]] = []
    for i in range(SESSION_POOL_SIZE):
        s = make_session()
        result = warm_session(s)
        result["session_index"] = i
        results.append(result)
        _SESSION_POOL.put(s)
        if i + 1 < SESSION_POOL_SIZE:
            time.sleep(0.15)
    return results


def acquire_session(deadline_monotonic: float) -> requests.Session | None:
    remaining = deadline_monotonic - time.monotonic() - CYCLE_GUARD_SECONDS
    if remaining <= 0:
        return None
    try:
        return _SESSION_POOL.get(timeout=min(remaining, 0.5))
    except queue.Empty:
        return None


def release_session(session: requests.Session | None) -> None:
    if session is not None:
        _SESSION_POOL.put(session)


def fetch_once(batch: list[Stock], timeout: float, stage: str, deadline_monotonic: float) -> FetchResult:
    started = time.monotonic()
    if not wait_for_global_backoff(deadline_monotonic):
        return FetchResult(batch, [], None, False, "global backoff exceeded cycle deadline",
                           "BACKOFF_DEADLINE", None, time.monotonic() - started, stage)

    session = acquire_session(deadline_monotonic)
    if session is None:
        return FetchResult(batch, [], None, False, "session pool unavailable before deadline",
                           "SESSION_POOL_TIMEOUT", None, time.monotonic() - started, stage)

    params = {
        "ex_ch": "|".join(stock.ex_ch for stock in batch),
        "json": "1",
        "delay": "0",
        "_": str(int(time.time() * 1000)),
    }

    try:
        remaining = deadline_monotonic - time.monotonic() - CYCLE_GUARD_SECONDS
        if remaining <= 0:
            raise requests.Timeout("cycle deadline exhausted before request")
        request_timeout = max(0.35, min(timeout, remaining))
        response = session.get(MIS_URL, params=params, timeout=request_timeout)

        if response.status_code in RETRYABLE_HTTP:
            err = requests.HTTPError(f"retryable HTTP {response.status_code}", response=response)
            raise err

        response.raise_for_status()
        obj = response.json()
        if not isinstance(obj, dict):
            raise RuntimeError("MIS response is not an object")
        if str(obj.get("rtcode", "")) != "0000":
            raise RuntimeError(f"MIS rtcode={obj.get('rtcode')} message={obj.get('rtmessage')}")
        items = obj.get("msgArray", []) or []
        if not isinstance(items, list):
            raise RuntimeError("MIS msgArray is not a list")

        register_success()
        return FetchResult(batch, items, obj, True, None, None, response.status_code,
                           time.monotonic() - started, stage)
    except Exception as exc:
        error_type, http_status = classify_exception(exc)
        register_backoff(error_type)
        return FetchResult(batch, [], None, False, f"{type(exc).__name__}: {exc}",
                           error_type, http_status, time.monotonic() - started, stage)
    finally:
        release_session(session)


def run_parallel(
    batches: list[list[Stock]],
    workers: int,
    timeout: float,
    stage: str,
    deadline_monotonic: float,
) -> list[FetchResult]:
    if not batches:
        return []

    remaining = deadline_monotonic - time.monotonic() - CYCLE_GUARD_SECONDS
    if remaining <= 0:
        return [
            FetchResult(b, [], None, False, "cycle deadline exhausted",
                        "CYCLE_DEADLINE", None, 0.0, stage)
            for b in batches
        ]

    actual_workers = max(1, min(workers, SESSION_POOL_SIZE, len(batches)))
    executor = ThreadPoolExecutor(max_workers=actual_workers, thread_name_prefix=f"a4-{stage}")
    futures = [
        executor.submit(fetch_once, batch, timeout, stage, deadline_monotonic)
        for batch in batches
    ]
    done, not_done = wait(
        futures,
        timeout=max(0.0, deadline_monotonic - time.monotonic() - CYCLE_GUARD_SECONDS),
    )

    results: list[FetchResult] = []
    for future in done:
        try:
            results.append(future.result())
        except Exception as exc:
            et, status = classify_exception(exc)
            results.append(FetchResult([], [], None, False, f"{type(exc).__name__}: {exc}",
                                       et, status, 0.0, stage))

    for future in not_done:
        future.cancel()
        results.append(FetchResult([], [], None, False, "future cancelled at cycle deadline",
                                   "FUTURE_DEADLINE", None, 0.0, stage))

    executor.shutdown(wait=False, cancel_futures=True)
    return results


def _merge_items(results: list[FetchResult], requested_codes: set[str], best: dict[str, dict[str, Any]]) -> None:
    for result in results:
        for item in result.items:
            code = str(item.get("c", "")).strip()
            if code and code in requested_codes:
                best[code] = item


def capture_cycle(stocks: list[Stock], cycle_deadline: float) -> tuple[list[FetchResult], dict[str, Any]]:
    by_code = {s.code: s for s in stocks}
    requested_codes = set(by_code)
    all_results: list[FetchResult] = []
    best_items: dict[str, dict[str, Any]] = {}

    primary = run_parallel(
        chunks(stocks, BATCH_SIZE),
        PRIMARY_WORKERS,
        PRIMARY_TIMEOUT,
        "primary",
        cycle_deadline,
    )
    all_results.extend(primary)
    _merge_items(primary, requested_codes, best_items)

    primary_coverage = len(best_items) / len(requested_codes) if requested_codes else 0.0
    missing = requested_codes - set(best_items)

    # Catastrophic primary failure: fail this cycle quietly.
    # Do not fan out into dozens of retry requests and create a request storm.
    allow_retry = primary_coverage >= RETRY_TRIGGER_COVERAGE

    if missing and allow_retry and time.monotonic() < cycle_deadline - CYCLE_GUARD_SECONDS:
        retry_stocks = [by_code[c] for c in sorted(missing)]
        retry = run_parallel(
            chunks(retry_stocks, RETRY_BATCH_SIZE),
            RETRY_WORKERS,
            RETRY_TIMEOUT,
            "retry_small_batch",
            cycle_deadline,
        )
        all_results.extend(retry)
        _merge_items(retry, requested_codes, best_items)

    missing = requested_codes - set(best_items)
    if (
        missing
        and allow_retry
        and len(missing) <= SINGLE_FALLBACK_LIMIT
        and time.monotonic() < cycle_deadline - CYCLE_GUARD_SECONDS
    ):
        singles = run_parallel(
            [[by_code[c]] for c in sorted(missing)],
            min(RETRY_WORKERS, len(missing)),
            SINGLE_TIMEOUT,
            "single_fallback",
            cycle_deadline,
        )
        all_results.extend(singles)
        _merge_items(singles, requested_codes, best_items)

    missing_final = sorted(requested_codes - set(best_items))
    error_types = Counter(r.error_type for r in all_results if not r.ok and r.error_type)
    sample_errors = []
    for r in all_results:
        if not r.ok and r.error:
            sample_errors.append({
                "stage": r.stage,
                "error_type": r.error_type,
                "http_status": r.http_status,
                "error": r.error[:300],
            })
        if len(sample_errors) >= 8:
            break

    diagnostics = {
        "version": VERSION,
        "request_attempts": len(all_results),
        "request_errors": sum(1 for r in all_results if not r.ok),
        "primary_requests": len(primary),
        "retry_requests": sum(1 for r in all_results if r.stage == "retry_small_batch"),
        "single_requests": sum(1 for r in all_results if r.stage == "single_fallback"),
        "primary_coverage": primary_coverage,
        "retry_allowed": allow_retry,
        "error_type_counts": dict(error_types),
        "sample_errors": sample_errors,
        "missing_symbols": missing_final,
        "returned_symbols": len(best_items),
        "requested_symbols": len(requested_codes),
        "cycle_elapsed_seconds": None,
    }

    merged = FetchResult(
        stocks,
        [best_items[c] for c in sorted(best_items)],
        None,
        not missing_final,
        None if not missing_final else f"missing symbols: {','.join(missing_final[:30])}",
        None if not missing_final else "MISSING_SYMBOLS",
        None,
        0.0,
        "merged_cycle",
    )
    return [merged] + all_results, diagnostics


def phase_for(t: dtime) -> str:
    if t < START_TIME:
        return "WAIT"
    if t < PREOPEN_END:
        return "PREOPEN"
    if t < END_TIME:
        return "OPEN_VALIDATION"
    return "DONE"


def normalize_item(item: dict[str, Any], captured_at: datetime, phase: str) -> dict[str, Any]:
    bid_price = parse_levels(item.get("b"), float)
    ask_price = parse_levels(item.get("a"), float)
    bid_volume = parse_levels(item.get("g"), int)
    ask_volume = parse_levels(item.get("f"), int)

    def lv(xs: list[Any], idx: int) -> Any:
        return xs[idx] if len(xs) > idx else None

    return {
        "captured_at": captured_at.isoformat(timespec="milliseconds"),
        "phase": phase,
        "market": str(item.get("ex", "")),
        "code": str(item.get("c", "")).strip(),
        "name": str(item.get("n", "")),
        "market_date": str(item.get("d", "")).strip(),
        "market_time": str(item.get("t", "")).strip(),
        "exchange_time": str(item.get("%", "")).strip(),
        "simulated_flag": str(item.get("ts", "")),
        "last_price": item.get("z"),
        "single_volume": item.get("tv"),
        "total_volume": item.get("v"),
        "reference_price": item.get("y"),
        "open": item.get("o"),
        "high": item.get("h"),
        "low": item.get("l"),
        "raw_pz": item.get("pz"),
        "raw_ps": item.get("ps"),
        "raw_oa": item.get("oa"),
        "raw_ob": item.get("ob"),
        "raw_ip": item.get("ip"),
        "bid_price_1": lv(bid_price, 0),
        "bid_price_2": lv(bid_price, 1),
        "bid_price_3": lv(bid_price, 2),
        "bid_price_4": lv(bid_price, 3),
        "bid_price_5": lv(bid_price, 4),
        "bid_volume_1": lv(bid_volume, 0),
        "bid_volume_2": lv(bid_volume, 1),
        "bid_volume_3": lv(bid_volume, 2),
        "bid_volume_4": lv(bid_volume, 3),
        "bid_volume_5": lv(bid_volume, 4),
        "ask_price_1": lv(ask_price, 0),
        "ask_price_2": lv(ask_price, 1),
        "ask_price_3": lv(ask_price, 2),
        "ask_price_4": lv(ask_price, 3),
        "ask_price_5": lv(ask_price, 4),
        "ask_volume_1": lv(ask_volume, 0),
        "ask_volume_2": lv(ask_volume, 1),
        "ask_volume_3": lv(ask_volume, 2),
        "ask_volume_4": lv(ask_volume, 3),
        "ask_volume_5": lv(ask_volume, 4),
    }


def expected_cycles(start: dtime, end: dtime, base: datetime) -> int:
    duration = (combine_today(end, base) - combine_today(start, base)).total_seconds()
    return max(0, int(math.ceil(duration / POLL_SECONDS)))


def max_gap_seconds(timestamps: list[datetime]) -> float | None:
    if len(timestamps) < 2:
        return None
    return max((b - a).total_seconds() for a, b in zip(timestamps, timestamps[1:]))


def run_env_test(stocks: list[Stock]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    started = now_tw()
    warmup = init_session_pool()
    cycle_start = time.monotonic()
    results, diag = capture_cycle(stocks, cycle_start + CYCLE_BUDGET_SECONDS)
    diag["cycle_elapsed_seconds"] = time.monotonic() - cycle_start
    merged = results[0]

    rows = []
    for item in merged.items:
        row = normalize_item(item, now_tw(), "ENV_TEST")
        if row["code"]:
            rows.append(row)

    requested_codes = {s.code for s in stocks}
    returned_codes = {r["code"] for r in rows if r.get("code")}
    missing = sorted(requested_codes - returned_codes)

    report = {
        "version": VERSION,
        "mode": "env_test",
        "started_at": started.isoformat(),
        "finished_at": now_tw().isoformat(),
        "requested_symbols": len(stocks),
        "returned_symbols": len(returned_codes),
        "missing_symbols": missing,
        "warmup_ok_count": sum(1 for x in warmup if x.get("ok")),
        "warmup_total": len(warmup),
        "warmup": warmup,
        "request_attempts": diag["request_attempts"],
        "request_errors": diag["request_errors"],
        "error_type_counts": diag["error_type_counts"],
        "sample_errors": diag["sample_errors"],
        "cycle_elapsed_seconds": diag["cycle_elapsed_seconds"],
        "cycle_budget_seconds": CYCLE_BUDGET_SECONDS,
        "pass": (
            len(returned_codes) >= math.ceil(len(stocks) * MIN_SYMBOL_COVERAGE)
            and diag["cycle_elapsed_seconds"] <= CYCLE_BUDGET_SECONDS + 0.25
        ),
    }

    atomic_write_json(OUTPUT_DIR / "env_test_report.json", report)
    atomic_write_json(OUTPUT_DIR / "env_test_diagnostics.json", diag)
    if rows:
        pd.DataFrame(rows).to_csv(OUTPUT_DIR / "env_test_normalized.csv", index=False, encoding="utf-8-sig")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 2


def run_live(stocks: list[Stock]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    process_start = now_tw()
    day = process_start.strftime("%Y-%m-%d")
    day_compact = process_start.strftime("%Y%m%d")

    raw_path = OUTPUT_DIR / f"raw_{day}.ndjson"
    norm_path = OUTPUT_DIR / f"normalized_{day}.parquet"
    csv_path = OUTPUT_DIR / f"normalized_{day}.csv"
    report_path = OUTPUT_DIR / f"report_{day}.json"
    heartbeat_path = OUTPUT_DIR / f"heartbeat_{day}.json"

    arm_at = combine_today(ARM_TIME, process_start)
    start_at = combine_today(START_TIME, process_start)
    end_at = combine_today(END_TIME, process_start)
    startup_deadline_at = combine_today(STARTUP_DEADLINE, process_start)
    first_preopen_deadline_at = combine_today(FIRST_PREOPEN_DEADLINE, process_start)
    last_preopen_earliest_at = combine_today(LAST_PREOPEN_EARLIEST, process_start)

    if process_start >= end_at:
        report = {
            "version": VERSION,
            "mode": "live",
            "date": day,
            "pass": False,
            "pass_candidate": False,
            "source_gap": True,
            "data_health": "SOURCE_GAP",
            "failure_reasons": ["STARTED_AFTER_END_TIME"],
            "actual_process_start": process_start.isoformat(),
        }
        atomic_write_json(report_path, report)
        print(json.dumps(report, ensure_ascii=False))
        return 3

    warmup = init_session_pool()

    while now_tw() < start_at:
        remaining = (start_at - now_tw()).total_seconds()
        time.sleep(max(0.05, min(1.0, remaining)))

    normalized: list[dict[str, Any]] = []
    preopen_valid_cycles: list[datetime] = []
    open_valid_cycles: list[datetime] = []
    all_valid_cycles: list[datetime] = []

    request_count = 0
    error_count = 0
    stale_row_count = 0
    rejected_blank_code_rows = 0
    incomplete_cycle_count = 0
    deadline_miss_count = 0
    preopen_rows = 0
    open_rows = 0
    max_cycle_elapsed = 0.0
    min_observed_symbol_coverage = 1.0
    consecutive_failed_cycles = 0
    max_consecutive_failed_cycles = 0
    first_capture = None
    last_capture = None
    aggregate_error_types: Counter[str] = Counter()
    aggregate_sample_errors: list[dict[str, Any]] = []

    requested_codes = {s.code for s in stocks}
    minimum_symbols_per_cycle = max(1, math.ceil(len(requested_codes) * MIN_SYMBOL_COVERAGE))
    cycle_index = max(0, int((now_tw() - start_at).total_seconds() // POLL_SECONDS))

    with raw_path.open("a", encoding="utf-8") as raw_file:
        while True:
            target = start_at + timedelta(seconds=cycle_index * POLL_SECONDS)
            if target >= end_at:
                break

            now = now_tw()
            if now < target:
                time.sleep(min(0.10, (target - now).total_seconds()))
                continue

            late_seconds = (now - target).total_seconds()
            if late_seconds >= POLL_SECONDS:
                skipped = int(late_seconds // POLL_SECONDS)
                deadline_miss_count += skipped
                incomplete_cycle_count += skipped
                consecutive_failed_cycles += skipped
                max_consecutive_failed_cycles = max(max_consecutive_failed_cycles, consecutive_failed_cycles)
                cycle_index += skipped
                continue

            captured = now_tw()
            phase = phase_for(captured.time())
            if phase == "DONE":
                break

            cycle_started = time.monotonic()
            results, diag = capture_cycle(stocks, cycle_started + CYCLE_BUDGET_SECONDS)
            cycle_elapsed = time.monotonic() - cycle_started
            diag.update(
                {
                    "cycle_elapsed_seconds": cycle_elapsed,
                    "cycle_index": cycle_index,
                    "target_at": target.isoformat(),
                    "captured_at": captured.isoformat(),
                    "phase": phase,
                }
            )
            max_cycle_elapsed = max(max_cycle_elapsed, cycle_elapsed)
            merged = results[0]
            request_count += diag["request_attempts"]
            error_count += diag["request_errors"]
            aggregate_error_types.update(diag["error_type_counts"])
            for err in diag["sample_errors"]:
                if len(aggregate_sample_errors) < 20:
                    aggregate_sample_errors.append(err)

            returned_codes_this_cycle: set[str] = set()
            fresh_codes_this_cycle: set[str] = set()

            for item in merged.items:
                row = normalize_item(item, captured, phase)
                code = row["code"]

                # Reject malformed blank-code rows before CLEAN persistence.
                if not code:
                    rejected_blank_code_rows += 1
                    continue
                if code not in requested_codes:
                    continue

                returned_codes_this_cycle.add(code)
                if row["market_date"] == day_compact:
                    fresh_codes_this_cycle.add(code)
                else:
                    stale_row_count += 1

                normalized.append(row)
                if phase == "PREOPEN":
                    preopen_rows += 1
                elif phase == "OPEN_VALIDATION":
                    open_rows += 1

            observed_coverage = (
                len(returned_codes_this_cycle) / len(requested_codes) if requested_codes else 0.0
            )
            min_observed_symbol_coverage = min(min_observed_symbol_coverage, observed_coverage)

            cycle_valid = len(fresh_codes_this_cycle) >= minimum_symbols_per_cycle
            if cycle_valid:
                all_valid_cycles.append(captured)
                if first_capture is None:
                    first_capture = captured.isoformat()
                last_capture = captured.isoformat()
                if phase == "PREOPEN":
                    preopen_valid_cycles.append(captured)
                elif phase == "OPEN_VALIDATION":
                    open_valid_cycles.append(captured)
                consecutive_failed_cycles = 0
            else:
                incomplete_cycle_count += 1
                consecutive_failed_cycles += 1

            max_consecutive_failed_cycles = max(max_consecutive_failed_cycles, consecutive_failed_cycles)
            source_gap_active = consecutive_failed_cycles >= SOURCE_GAP_CONSECUTIVE_CYCLES

            raw_file.write(
                json.dumps(
                    {
                        "captured_at": captured.isoformat(timespec="milliseconds"),
                        "phase": phase,
                        "cycle_index": cycle_index,
                        "target_at": target.isoformat(),
                        "diagnostics": diag,
                        "items": merged.items,
                    },
                    ensure_ascii=False,
                )
                + "\n"
            )
            raw_file.flush()

            atomic_write_json(
                heartbeat_path,
                {
                    "version": VERSION,
                    "date": day,
                    "captured_at": captured.isoformat(),
                    "phase": phase,
                    "cycle_index": cycle_index,
                    "valid_cycle": cycle_valid,
                    "cycle_elapsed_seconds": cycle_elapsed,
                    "cycle_budget_seconds": CYCLE_BUDGET_SECONDS,
                    "returned_symbols": len(returned_codes_this_cycle),
                    "fresh_symbols": len(fresh_codes_this_cycle),
                    "requested_symbols": len(requested_codes),
                    "missing_symbols": diag["missing_symbols"],
                    "request_count": request_count,
                    "error_count": error_count,
                    "error_type_counts": dict(aggregate_error_types),
                    "consecutive_failed_cycles": consecutive_failed_cycles,
                    "max_consecutive_failed_cycles": max_consecutive_failed_cycles,
                    "source_gap": source_gap_active,
                    "data_health": "SOURCE_GAP" if source_gap_active else "NORMAL",
                },
            )
            cycle_index += 1

    if normalized:
        df = pd.DataFrame(normalized)
        df.to_parquet(norm_path, index=False)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    expected_preopen = expected_cycles(START_TIME, PREOPEN_END, process_start)
    expected_open = expected_cycles(PREOPEN_END, END_TIME, process_start)
    preopen_coverage = len(preopen_valid_cycles) / expected_preopen if expected_preopen else 0.0
    open_coverage = len(open_valid_cycles) / expected_open if expected_open else 0.0
    observed_max_gap = max_gap_seconds(all_valid_cycles)
    request_error_rate = error_count / request_count if request_count else 1.0
    first_preopen = preopen_valid_cycles[0] if preopen_valid_cycles else None
    last_preopen = preopen_valid_cycles[-1] if preopen_valid_cycles else None

    gates = {
        "startup_before_082945": process_start <= startup_deadline_at,
        "first_preopen_by_083020": bool(first_preopen and first_preopen <= first_preopen_deadline_at),
        "last_preopen_at_or_after_085940": bool(last_preopen and last_preopen >= last_preopen_earliest_at),
        "preopen_coverage_ok": preopen_coverage >= MIN_PREOPEN_COVERAGE,
        "open_validation_coverage_ok": open_coverage >= MIN_OPEN_COVERAGE,
        "max_gap_ok": observed_max_gap is not None and observed_max_gap <= MAX_ALLOWED_GAP_SECONDS,
        "request_error_rate_ok": request_error_rate <= MAX_REQUEST_ERROR_RATE,
        "source_gap_not_persistent": max_consecutive_failed_cycles < SOURCE_GAP_CONSECUTIVE_CYCLES,
        "fresh_market_date_only": stale_row_count == 0,
        "symbol_coverage_ok": min_observed_symbol_coverage >= MIN_SYMBOL_COVERAGE,
        "cycle_budget_ok": max_cycle_elapsed <= CYCLE_BUDGET_SECONDS + 0.25,
        "warmup_available": any(x.get("ok") for x in warmup),
    }
    failure_reasons = [name.upper() for name, passed in gates.items() if not passed]
    passed = all(gates.values())

    report = {
        "version": VERSION,
        "mode": "live",
        "date": day,
        "actual_process_start": process_start.isoformat(),
        "schedule_delay_seconds_from_0825": max(0.0, (process_start - arm_at).total_seconds()),
        "first_capture": first_capture,
        "last_capture": last_capture,
        "first_preopen_valid_capture": first_preopen.isoformat() if first_preopen else None,
        "last_preopen_valid_capture": last_preopen.isoformat() if last_preopen else None,
        "poll_seconds": POLL_SECONDS,
        "cycle_budget_seconds": CYCLE_BUDGET_SECONDS,
        "max_cycle_elapsed_seconds": max_cycle_elapsed,
        "deadline_miss_count": deadline_miss_count,
        "requested_symbols": len(stocks),
        "requests": request_count,
        "error_count": error_count,
        "request_error_rate": request_error_rate,
        "allowed_request_error_rate": MAX_REQUEST_ERROR_RATE,
        "error_type_counts": dict(aggregate_error_types),
        "sample_errors": aggregate_sample_errors,
        "normalized_rows": len(normalized),
        "preopen_rows": preopen_rows,
        "open_validation_rows": open_rows,
        "stale_row_count": stale_row_count,
        "rejected_blank_code_rows": rejected_blank_code_rows,
        "incomplete_cycle_count": incomplete_cycle_count,
        "max_consecutive_failed_cycles": max_consecutive_failed_cycles,
        "source_gap_threshold_cycles": SOURCE_GAP_CONSECUTIVE_CYCLES,
        "source_gap": not gates["source_gap_not_persistent"],
        "data_health": "NORMAL" if gates["source_gap_not_persistent"] else "SOURCE_GAP",
        "expected_preopen_cycles": expected_preopen,
        "actual_preopen_valid_cycles": len(preopen_valid_cycles),
        "preopen_coverage": preopen_coverage,
        "required_preopen_coverage": MIN_PREOPEN_COVERAGE,
        "expected_open_validation_cycles": expected_open,
        "actual_open_validation_valid_cycles": len(open_valid_cycles),
        "open_validation_coverage": open_coverage,
        "required_open_validation_coverage": MIN_OPEN_COVERAGE,
        "min_observed_symbol_coverage": min_observed_symbol_coverage,
        "required_symbol_coverage": MIN_SYMBOL_COVERAGE,
        "max_gap_seconds": observed_max_gap,
        "allowed_max_gap_seconds": MAX_ALLOWED_GAP_SECONDS,
        "warmup_ok_count": sum(1 for x in warmup if x.get("ok")),
        "warmup_total": len(warmup),
        "warmup": warmup,
        "session_pool_size": SESSION_POOL_SIZE,
        "retry_trigger_coverage": RETRY_TRIGGER_COVERAGE,
        "gates": gates,
        "failure_reasons": failure_reasons,
        "pass_candidate": passed,
        "pass": passed,
        "raw_file": str(raw_path),
        "normalized_parquet": str(norm_path),
        "normalized_csv": str(csv_path),
        "heartbeat_file": str(heartbeat_path),
    }

    atomic_write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if passed else 4


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
            {
                "version": VERSION,
                "at": now_tw().isoformat(),
                "error": repr(exc),
                "run_mode": RUN_MODE,
                "source_gap": True,
                "data_health": "SOURCE_GAP",
            },
        )
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(99)
