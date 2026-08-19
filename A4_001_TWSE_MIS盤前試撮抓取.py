from __future__ import annotations

import csv
import json
import math
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

VERSION = "1.4.0"
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

BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "40")))
PRIMARY_WORKERS = max(1, int(os.getenv("PRIMARY_WORKERS", "10")))
RETRY_BATCH_SIZE = max(1, int(os.getenv("RETRY_BATCH_SIZE", "12")))
RETRY_WORKERS = max(1, int(os.getenv("RETRY_WORKERS", "12")))
SINGLE_FALLBACK_LIMIT = max(0, int(os.getenv("SINGLE_FALLBACK_LIMIT", "12")))

PRIMARY_TIMEOUT = max(0.35, float(os.getenv("PRIMARY_TIMEOUT", "1.35")))
RETRY_TIMEOUT = max(0.30, float(os.getenv("RETRY_TIMEOUT", "0.90")))
SINGLE_TIMEOUT = max(0.25, float(os.getenv("SINGLE_TIMEOUT", "0.60")))
CYCLE_BUDGET_SECONDS = max(1.0, float(os.getenv("CYCLE_BUDGET_SECONDS", "4.60")))
CYCLE_GUARD_SECONDS = max(0.05, float(os.getenv("CYCLE_GUARD_SECONDS", "0.15")))
WARMUP_TIMEOUT = max(0.5, float(os.getenv("WARMUP_TIMEOUT", "2.0")))

SOURCE_GAP_CONSECUTIVE_CYCLES = max(1, int(os.getenv("SOURCE_GAP_CONSECUTIVE_CYCLES", "3")))
MAX_REQUEST_ERROR_RATE = min(1.0, max(0.0, float(os.getenv("MAX_REQUEST_ERROR_RATE", "0.02"))))
MIN_PREOPEN_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_PREOPEN_COVERAGE", "0.98"))))
MIN_OPEN_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_OPEN_COVERAGE", "0.95"))))
MIN_SYMBOL_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_SYMBOL_COVERAGE", "1.0"))))
MAX_ALLOWED_GAP_SECONDS = max(POLL_SECONDS, float(os.getenv("MAX_ALLOWED_GAP_SECONDS", "15")))

RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
_THREAD_LOCAL = threading.local()


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
    elapsed_seconds: float
    stage: str


def now_tw() -> datetime:
    return datetime.now(TZ)


def combine_today(t: dtime, base: datetime | None = None) -> datetime:
    base = base or now_tw()
    return datetime.combine(base.date(), t, TZ)


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
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max(PRIMARY_WORKERS, RETRY_WORKERS) + 4,
        pool_maxsize=max(PRIMARY_WORKERS, RETRY_WORKERS) + 4,
        max_retries=0,
        pool_block=False,
    )
    session.mount("https://", adapter)
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36 QTS-A4/1.4",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
            "Referer": MIS_FIBEST_URL,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
    )
    return session


def thread_session() -> requests.Session:
    session = getattr(_THREAD_LOCAL, "session", None)
    if session is None:
        session = make_session()
        _THREAD_LOCAL.session = session
    return session


def warmup_one() -> dict[str, Any]:
    session = thread_session()
    last_error = None
    for url in (MIS_HOME_URL, MIS_FIBEST_URL):
        try:
            r = session.get(url, timeout=WARMUP_TIMEOUT, allow_redirects=True)
            if 200 <= r.status_code < 400:
                return {"ok": True, "status": r.status_code, "url": url}
            last_error = f"HTTP {r.status_code}"
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
    return {"ok": False, "error": last_error}


def warmup_pool() -> list[dict[str, Any]]:
    workers = max(PRIMARY_WORKERS, RETRY_WORKERS)
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="a4-warm") as executor:
        futures = [executor.submit(warmup_one) for _ in range(workers)]
        return [f.result() for f in futures]


def fetch_once(batch: list[Stock], timeout: float, stage: str) -> FetchResult:
    started = time.monotonic()
    params = {
        "ex_ch": "|".join(stock.ex_ch for stock in batch),
        "json": "1",
        "delay": "0",
        "_": str(int(time.time() * 1000)),
    }
    try:
        response = thread_session().get(MIS_URL, params=params, timeout=timeout)
        if response.status_code in RETRYABLE_HTTP:
            raise requests.HTTPError(f"retryable HTTP {response.status_code}", response=response)
        response.raise_for_status()
        obj = response.json()
        if not isinstance(obj, dict):
            raise RuntimeError("MIS response is not an object")
        if str(obj.get("rtcode", "")) != "0000":
            raise RuntimeError(f"MIS rtcode={obj.get('rtcode')} message={obj.get('rtmessage')}")
        items = obj.get("msgArray", []) or []
        if not isinstance(items, list):
            raise RuntimeError("MIS msgArray is not a list")
        return FetchResult(batch, items, obj, True, None, time.monotonic() - started, stage)
    except Exception as exc:
        return FetchResult(batch, [], None, False, f"{type(exc).__name__}: {exc}", time.monotonic() - started, stage)


def run_parallel(batches: list[list[Stock]], workers: int, timeout: float, stage: str, deadline_monotonic: float) -> list[FetchResult]:
    if not batches:
        return []
    remaining = deadline_monotonic - time.monotonic() - CYCLE_GUARD_SECONDS
    if remaining <= 0:
        return [FetchResult(b, [], None, False, "cycle deadline exhausted", 0.0, stage) for b in batches]

    request_timeout = max(0.20, min(timeout, remaining))
    executor = ThreadPoolExecutor(max_workers=min(workers, len(batches)), thread_name_prefix=f"a4-{stage}")
    futures = [executor.submit(fetch_once, batch, request_timeout, stage) for batch in batches]
    done, not_done = wait(futures, timeout=max(0.0, deadline_monotonic - time.monotonic() - CYCLE_GUARD_SECONDS))

    results: list[FetchResult] = []
    for future in done:
        try:
            results.append(future.result())
        except Exception as exc:
            results.append(FetchResult([], [], None, False, f"{type(exc).__name__}: {exc}", 0.0, stage))
    for future in not_done:
        future.cancel()

    executor.shutdown(wait=False, cancel_futures=True)
    return results


def capture_cycle(stocks: list[Stock], cycle_deadline: float) -> tuple[list[FetchResult], dict[str, Any]]:
    by_code = {s.code: s for s in stocks}
    requested_codes = set(by_code)
    all_results: list[FetchResult] = []
    best_items: dict[str, dict[str, Any]] = {}

    primary = run_parallel(chunks(stocks, BATCH_SIZE), PRIMARY_WORKERS, PRIMARY_TIMEOUT, "primary", cycle_deadline)
    all_results.extend(primary)
    for result in primary:
        for item in result.items:
            code = str(item.get("c", "")).strip()
            if code in requested_codes:
                best_items[code] = item

    missing = requested_codes - set(best_items)
    if missing and time.monotonic() < cycle_deadline - CYCLE_GUARD_SECONDS:
        retry_stocks = [by_code[c] for c in sorted(missing)]
        retry = run_parallel(chunks(retry_stocks, RETRY_BATCH_SIZE), RETRY_WORKERS, RETRY_TIMEOUT, "retry_small_batch", cycle_deadline)
        all_results.extend(retry)
        for result in retry:
            for item in result.items:
                code = str(item.get("c", "")).strip()
                if code in requested_codes:
                    best_items[code] = item

    missing = requested_codes - set(best_items)
    if missing and len(missing) <= SINGLE_FALLBACK_LIMIT and time.monotonic() < cycle_deadline - CYCLE_GUARD_SECONDS:
        singles = run_parallel([[by_code[c]] for c in sorted(missing)], min(RETRY_WORKERS, len(missing)), SINGLE_TIMEOUT, "single_fallback", cycle_deadline)
        all_results.extend(singles)
        for result in singles:
            for item in result.items:
                code = str(item.get("c", "")).strip()
                if code in requested_codes:
                    best_items[code] = item

    missing_final = sorted(requested_codes - set(best_items))
    diagnostics = {
        "version": VERSION,
        "request_attempts": len(all_results),
        "request_errors": sum(1 for r in all_results if not r.ok),
        "primary_requests": len(primary),
        "retry_requests": sum(1 for r in all_results if r.stage == "retry_small_batch"),
        "single_requests": sum(1 for r in all_results if r.stage == "single_fallback"),
        "missing_symbols": missing_final,
        "returned_symbols": len(best_items),
        "requested_symbols": len(requested_codes),
        "cycle_elapsed_seconds": None,
    }
    merged = FetchResult(stocks, [best_items[c] for c in sorted(best_items)], None, not missing_final, None if not missing_final else f"missing symbols: {','.join(missing_final)}", 0.0, "merged_cycle")
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
        "captured_at": captured_at.isoformat(timespec="milliseconds"), "phase": phase,
        "market": str(item.get("ex", "")), "code": str(item.get("c", "")), "name": str(item.get("n", "")),
        "market_date": str(item.get("d", "")), "market_time": str(item.get("t", "")), "exchange_time": str(item.get("%", "")),
        "simulated_flag": str(item.get("ts", "")), "last_price": item.get("z"), "single_volume": item.get("tv"),
        "total_volume": item.get("v"), "reference_price": item.get("y"), "open": item.get("o"), "high": item.get("h"), "low": item.get("l"),
        "raw_pz": item.get("pz"), "raw_ps": item.get("ps"), "raw_oa": item.get("oa"), "raw_ob": item.get("ob"), "raw_ip": item.get("ip"),
        "bid_price_1": lv(bid_price, 0), "bid_price_2": lv(bid_price, 1), "bid_price_3": lv(bid_price, 2), "bid_price_4": lv(bid_price, 3), "bid_price_5": lv(bid_price, 4),
        "bid_volume_1": lv(bid_volume, 0), "bid_volume_2": lv(bid_volume, 1), "bid_volume_3": lv(bid_volume, 2), "bid_volume_4": lv(bid_volume, 3), "bid_volume_5": lv(bid_volume, 4),
        "ask_price_1": lv(ask_price, 0), "ask_price_2": lv(ask_price, 1), "ask_price_3": lv(ask_price, 2), "ask_price_4": lv(ask_price, 3), "ask_price_5": lv(ask_price, 4),
        "ask_volume_1": lv(ask_volume, 0), "ask_volume_2": lv(ask_volume, 1), "ask_volume_3": lv(ask_volume, 2), "ask_volume_4": lv(ask_volume, 3), "ask_volume_5": lv(ask_volume, 4),
    }


def atomic_write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


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
    warmup = warmup_pool()
    cycle_start = time.monotonic()
    results, diag = capture_cycle(stocks, cycle_start + CYCLE_BUDGET_SECONDS)
    diag["cycle_elapsed_seconds"] = time.monotonic() - cycle_start
    merged = results[0]
    rows = [normalize_item(item, now_tw(), "ENV_TEST") for item in merged.items]
    requested_codes = {s.code for s in stocks}
    returned_codes = {r["code"] for r in rows if r.get("code")}
    missing = sorted(requested_codes - returned_codes)
    report = {
        "version": VERSION, "mode": "env_test", "started_at": started.isoformat(), "finished_at": now_tw().isoformat(),
        "requested_symbols": len(stocks), "returned_symbols": len(returned_codes), "missing_symbols": missing,
        "warmup_ok_count": sum(1 for x in warmup if x.get("ok")), "warmup_total": len(warmup),
        "request_attempts": diag["request_attempts"], "request_errors": diag["request_errors"],
        "cycle_elapsed_seconds": diag["cycle_elapsed_seconds"], "cycle_budget_seconds": CYCLE_BUDGET_SECONDS,
        "pass": not missing and diag["cycle_elapsed_seconds"] <= CYCLE_BUDGET_SECONDS + 0.25,
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
        report = {"version": VERSION, "mode": "live", "date": day, "pass": False, "pass_candidate": False, "source_gap": True, "data_health": "SOURCE_GAP", "failure_reasons": ["STARTED_AFTER_END_TIME"], "actual_process_start": process_start.isoformat()}
        atomic_write_json(report_path, report)
        print(json.dumps(report, ensure_ascii=False))
        return 3

    warmup = warmup_pool()
    while now_tw() < start_at:
        remaining = (start_at - now_tw()).total_seconds()
        time.sleep(max(0.05, min(1.0, remaining)))

    normalized: list[dict[str, Any]] = []
    preopen_valid_cycles: list[datetime] = []
    open_valid_cycles: list[datetime] = []
    all_valid_cycles: list[datetime] = []
    request_count = error_count = stale_row_count = incomplete_cycle_count = deadline_miss_count = 0
    preopen_rows = open_rows = 0
    max_cycle_elapsed = 0.0
    min_observed_symbol_coverage = 1.0
    consecutive_failed_cycles = max_consecutive_failed_cycles = 0
    first_capture = last_capture = None
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
            diag.update({"cycle_elapsed_seconds": cycle_elapsed, "cycle_index": cycle_index, "target_at": target.isoformat(), "captured_at": captured.isoformat(), "phase": phase})
            max_cycle_elapsed = max(max_cycle_elapsed, cycle_elapsed)
            merged = results[0]
            request_count += diag["request_attempts"]
            error_count += diag["request_errors"]

            returned_codes_this_cycle: set[str] = set()
            fresh_codes_this_cycle: set[str] = set()
            for item in merged.items:
                row = normalize_item(item, captured, phase)
                code = row["code"]
                if code in requested_codes:
                    returned_codes_this_cycle.add(code)
                if row["market_date"] == day_compact:
                    if code in requested_codes:
                        fresh_codes_this_cycle.add(code)
                else:
                    stale_row_count += 1
                normalized.append(row)
                if phase == "PREOPEN": preopen_rows += 1
                elif phase == "OPEN_VALIDATION": open_rows += 1

            observed_coverage = len(returned_codes_this_cycle) / len(requested_codes) if requested_codes else 0.0
            min_observed_symbol_coverage = min(min_observed_symbol_coverage, observed_coverage)
            cycle_valid = len(fresh_codes_this_cycle) >= minimum_symbols_per_cycle
            if cycle_valid:
                all_valid_cycles.append(captured)
                if first_capture is None: first_capture = captured.isoformat()
                last_capture = captured.isoformat()
                if phase == "PREOPEN": preopen_valid_cycles.append(captured)
                elif phase == "OPEN_VALIDATION": open_valid_cycles.append(captured)
                consecutive_failed_cycles = 0
            else:
                incomplete_cycle_count += 1
                consecutive_failed_cycles += 1
            max_consecutive_failed_cycles = max(max_consecutive_failed_cycles, consecutive_failed_cycles)
            source_gap_active = consecutive_failed_cycles >= SOURCE_GAP_CONSECUTIVE_CYCLES

            raw_file.write(json.dumps({"captured_at": captured.isoformat(timespec="milliseconds"), "phase": phase, "cycle_index": cycle_index, "target_at": target.isoformat(), "diagnostics": diag, "items": merged.items}, ensure_ascii=False) + "\n")
            raw_file.flush()
            atomic_write_json(heartbeat_path, {"version": VERSION, "date": day, "captured_at": captured.isoformat(), "phase": phase, "cycle_index": cycle_index, "valid_cycle": cycle_valid, "cycle_elapsed_seconds": cycle_elapsed, "cycle_budget_seconds": CYCLE_BUDGET_SECONDS, "returned_symbols": len(returned_codes_this_cycle), "fresh_symbols": len(fresh_codes_this_cycle), "requested_symbols": len(requested_codes), "missing_symbols": diag["missing_symbols"], "request_count": request_count, "error_count": error_count, "consecutive_failed_cycles": consecutive_failed_cycles, "max_consecutive_failed_cycles": max_consecutive_failed_cycles, "source_gap": source_gap_active, "data_health": "SOURCE_GAP" if source_gap_active else "NORMAL"})
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
    }
    failure_reasons = [name.upper() for name, passed in gates.items() if not passed]
    passed = all(gates.values())
    report = {
        "version": VERSION, "mode": "live", "date": day, "actual_process_start": process_start.isoformat(),
        "schedule_delay_seconds_from_0825": max(0.0, (process_start - arm_at).total_seconds()), "first_capture": first_capture, "last_capture": last_capture,
        "first_preopen_valid_capture": first_preopen.isoformat() if first_preopen else None, "last_preopen_valid_capture": last_preopen.isoformat() if last_preopen else None,
        "poll_seconds": POLL_SECONDS, "cycle_budget_seconds": CYCLE_BUDGET_SECONDS, "max_cycle_elapsed_seconds": max_cycle_elapsed, "deadline_miss_count": deadline_miss_count,
        "requested_symbols": len(stocks), "requests": request_count, "error_count": error_count, "request_error_rate": request_error_rate, "allowed_request_error_rate": MAX_REQUEST_ERROR_RATE,
        "normalized_rows": len(normalized), "preopen_rows": preopen_rows, "open_validation_rows": open_rows, "stale_row_count": stale_row_count, "incomplete_cycle_count": incomplete_cycle_count,
        "max_consecutive_failed_cycles": max_consecutive_failed_cycles, "source_gap_threshold_cycles": SOURCE_GAP_CONSECUTIVE_CYCLES,
        "source_gap": not gates["source_gap_not_persistent"], "data_health": "NORMAL" if gates["source_gap_not_persistent"] else "SOURCE_GAP",
        "expected_preopen_cycles": expected_preopen, "actual_preopen_valid_cycles": len(preopen_valid_cycles), "preopen_coverage": preopen_coverage, "required_preopen_coverage": MIN_PREOPEN_COVERAGE,
        "expected_open_validation_cycles": expected_open, "actual_open_validation_valid_cycles": len(open_valid_cycles), "open_validation_coverage": open_coverage, "required_open_validation_coverage": MIN_OPEN_COVERAGE,
        "min_observed_symbol_coverage": min_observed_symbol_coverage, "required_symbol_coverage": MIN_SYMBOL_COVERAGE,
        "max_gap_seconds": observed_max_gap, "allowed_max_gap_seconds": MAX_ALLOWED_GAP_SECONDS,
        "warmup_ok_count": sum(1 for x in warmup if x.get("ok")), "warmup_total": len(warmup),
        "gates": gates, "failure_reasons": failure_reasons, "pass_candidate": passed, "pass": passed,
        "raw_file": str(raw_path), "normalized_parquet": str(norm_path), "normalized_csv": str(csv_path), "heartbeat_file": str(heartbeat_path),
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
        atomic_write_json(OUTPUT_DIR / "fatal_error.json", {"version": VERSION, "at": now_tw().isoformat(), "error": repr(exc), "run_mode": RUN_MODE, "source_gap": True, "data_health": "SOURCE_GAP"})
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(99)
