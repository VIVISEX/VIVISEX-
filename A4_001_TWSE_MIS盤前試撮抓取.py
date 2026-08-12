from __future__ import annotations

import csv
import json
import math
import os
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, time as dtime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

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
BATCH_SIZE = max(1, int(os.getenv("BATCH_SIZE", "50")))
MAX_RETRIES = max(1, int(os.getenv("MAX_RETRIES", "4")))
REQUEST_TIMEOUT = max(2.0, float(os.getenv("REQUEST_TIMEOUT", "5")))
RETRY_BASE_SECONDS = max(0.05, float(os.getenv("RETRY_BASE_SECONDS", "0.35")))
RETRY_CAP_SECONDS = max(RETRY_BASE_SECONDS, float(os.getenv("RETRY_CAP_SECONDS", "2.0")))
WARMUP_TIMEOUT = max(1.0, float(os.getenv("WARMUP_TIMEOUT", "4")))
SINGLE_FALLBACK_RETRIES = max(1, int(os.getenv("SINGLE_FALLBACK_RETRIES", "2")))
SINGLE_FALLBACK_TIMEOUT = max(2.0, float(os.getenv("SINGLE_FALLBACK_TIMEOUT", "4")))
ENABLE_SINGLE_FALLBACK = os.getenv("ENABLE_SINGLE_FALLBACK", "1").strip().lower() not in {"0", "false", "no"}
SOURCE_GAP_CONSECUTIVE_CYCLES = max(1, int(os.getenv("SOURCE_GAP_CONSECUTIVE_CYCLES", "3")))
MAX_REQUEST_ERROR_RATE = min(1.0, max(0.0, float(os.getenv("MAX_REQUEST_ERROR_RATE", "0.02"))))
MIN_PREOPEN_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_PREOPEN_COVERAGE", "0.98"))))
MIN_OPEN_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_OPEN_COVERAGE", "0.95"))))
MIN_SYMBOL_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_SYMBOL_COVERAGE", "1.0"))))
MAX_ALLOWED_GAP_SECONDS = max(POLL_SECONDS, float(os.getenv("MAX_ALLOWED_GAP_SECONDS", "15")))
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}


@dataclass(frozen=True)
class Stock:
    market: str
    code: str
    ex_ch: str


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
        for row in csv.DictReader(f):
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


def chunks(seq: list[Stock], n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def make_session(warmup: bool = True) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/151 Safari/537.36 QTS-A4/1.3",
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
            "Referer": MIS_FIBEST_URL,
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Connection": "keep-alive",
        }
    )
    if warmup:
        warmup_session(session)
    return session


def warmup_session(session: requests.Session) -> dict[str, Any]:
    result: dict[str, Any] = {"ok": False, "url": None, "status": None, "error": None}
    for url in (MIS_HOME_URL, MIS_FIBEST_URL):
        try:
            response = session.get(url, timeout=WARMUP_TIMEOUT, allow_redirects=True)
            result.update({"url": url, "status": response.status_code})
            if 200 <= response.status_code < 400:
                result["ok"] = True
                return result
        except requests.RequestException as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
    return result


def _retry_sleep(attempt: int) -> None:
    base = min(RETRY_CAP_SECONDS, RETRY_BASE_SECONDS * (2 ** max(0, attempt - 1)))
    time.sleep(base + random.uniform(0.0, min(0.25, RETRY_BASE_SECONDS)))


def _fetch_core(
    session: requests.Session,
    batch: list[Stock],
    max_retries: int,
    request_timeout: float,
) -> dict[str, Any]:
    last: Exception | None = None
    active_session = session

    for attempt in range(1, max_retries + 1):
        params = {
            "ex_ch": "|".join(stock.ex_ch for stock in batch),
            "json": "1",
            "delay": "0",
            "_": str(int(time.time() * 1000)),
        }
        try:
            response = active_session.get(MIS_URL, params=params, timeout=request_timeout)
            if response.status_code in RETRYABLE_HTTP:
                raise requests.HTTPError(
                    f"retryable HTTP {response.status_code}", response=response
                )
            response.raise_for_status()
            obj = response.json()
            if not isinstance(obj, dict):
                raise RuntimeError("MIS response is not an object")
            if str(obj.get("rtcode", "")) != "0000":
                raise RuntimeError(
                    f"MIS rtcode={obj.get('rtcode')} message={obj.get('rtmessage')}"
                )
            return obj
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, ValueError, RuntimeError) as exc:
            last = exc
            status = getattr(getattr(exc, "response", None), "status_code", None)
            non_retryable_http = status is not None and status not in RETRYABLE_HTTP
            if non_retryable_http or attempt >= max_retries:
                break

            if attempt >= 2:
                try:
                    active_session.close()
                except Exception:
                    pass
                active_session = make_session(warmup=False)
            _retry_sleep(attempt)

    raise RuntimeError(
        f"MIS request failed after {max_retries} attempts; symbols={[s.code for s in batch]}; "
        f"last={type(last).__name__ if last else 'Unknown'}: {last}"
    )


def _single_symbol_fetch(stock: Stock) -> tuple[str, dict[str, Any]]:
    session = make_session(warmup=False)
    try:
        raw = _fetch_core(
            session,
            [stock],
            max_retries=SINGLE_FALLBACK_RETRIES,
            request_timeout=SINGLE_FALLBACK_TIMEOUT,
        )
        return stock.code, raw
    finally:
        session.close()


def _merge_single_results(batch: list[Stock], results: dict[str, dict[str, Any]]) -> dict[str, Any]:
    merged_items: list[dict[str, Any]] = []
    query_time: dict[str, Any] = {}
    for stock in batch:
        raw = results[stock.code]
        items = raw.get("msgArray", []) or []
        matched = [item for item in items if str(item.get("c", "")) == stock.code]
        if not matched:
            raise RuntimeError(f"single fallback missing requested symbol={stock.code}")
        merged_items.extend(matched)
        if isinstance(raw.get("queryTime"), dict):
            query_time = raw["queryTime"]

    return {
        "rtcode": "0000",
        "rtmessage": "OK",
        "msgArray": merged_items,
        "queryTime": query_time,
        "_qts_transport": {
            "mode": "single_symbol_parallel_fallback",
            "symbols": [stock.code for stock in batch],
            "parts": len(batch),
        },
    }


def fetch_batch(session: requests.Session, batch: list[Stock]) -> dict[str, Any]:
    try:
        raw = _fetch_core(
            session,
            batch,
            max_retries=MAX_RETRIES,
            request_timeout=REQUEST_TIMEOUT,
        )
        raw.setdefault(
            "_qts_transport",
            {"mode": "batch", "symbols": [stock.code for stock in batch], "parts": 1},
        )
        return raw
    except Exception as primary_error:
        if not ENABLE_SINGLE_FALLBACK or len(batch) <= 1:
            raise RuntimeError(f"SOURCE_GAP: {primary_error}") from primary_error

        results: dict[str, dict[str, Any]] = {}
        failures: dict[str, str] = {}
        workers = min(5, len(batch))
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="a4-mis") as executor:
            future_map = {executor.submit(_single_symbol_fetch, stock): stock for stock in batch}
            for future in as_completed(future_map):
                stock = future_map[future]
                try:
                    code, raw = future.result()
                    results[code] = raw
                except Exception as exc:
                    failures[stock.code] = f"{type(exc).__name__}: {exc}"

        if failures or len(results) != len(batch):
            missing = sorted({stock.code for stock in batch} - set(results))
            raise RuntimeError(
                "SOURCE_GAP: batch request failed and single-symbol fallback incomplete; "
                f"primary={primary_error}; failures={failures}; missing={missing}"
            ) from primary_error

        return _merge_single_results(batch, results)


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

    return {
        "captured_at": captured_at.isoformat(timespec="milliseconds"),
        "phase": phase,
        "market": str(item.get("ex", "")),
        "code": str(item.get("c", "")),
        "name": str(item.get("n", "")),
        "market_date": str(item.get("d", "")),
        "market_time": str(item.get("t", "")),
        "exchange_time": str(item.get("%", "")),
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
    session = make_session(warmup=True)
    started = now_tw()
    errors: list[str] = []
    items: list[dict[str, Any]] = []
    raws: list[dict[str, Any]] = []
    requested_codes = {stock.code for stock in stocks}

    for batch in chunks(stocks, BATCH_SIZE):
        try:
            raw = fetch_batch(session, batch)
            raws.append(raw)
            for item in raw.get("msgArray", []) or []:
                items.append(normalize_item(item, now_tw(), "ENV_TEST"))
        except Exception as exc:
            errors.append(str(exc))

    returned_codes = {str(row.get("code", "")) for row in items if row.get("code")}
    missing_symbols = sorted(requested_codes - returned_codes)
    unexpected_symbols = sorted(returned_codes - requested_codes)
    passed = (
        not errors
        and not missing_symbols
        and not unexpected_symbols
        and len(returned_codes) == len(requested_codes)
    )

    report = {
        "mode": "env_test",
        "started_at": started.isoformat(),
        "finished_at": now_tw().isoformat(),
        "requested_symbols": len(stocks),
        "normalized_rows": len(items),
        "returned_symbols": len(returned_codes),
        "missing_symbols": missing_symbols,
        "unexpected_symbols": unexpected_symbols,
        "errors": errors,
        "source_gap": bool(errors),
        "transport_modes": [raw.get("_qts_transport", {}).get("mode") for raw in raws],
        "pass": passed,
    }
    atomic_write_json(OUTPUT_DIR / "env_test_report.json", report)
    atomic_write_json(OUTPUT_DIR / "env_test_raw.json", raws)
    if items:
        pd.DataFrame(items).to_csv(
            OUTPUT_DIR / "env_test_normalized.csv", index=False, encoding="utf-8-sig"
        )

    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 2


def expected_cycles(start: dtime, end: dtime, base: datetime) -> int:
    duration = (combine_today(end, base) - combine_today(start, base)).total_seconds()
    return max(0, int(math.ceil(duration / POLL_SECONDS)))


def max_gap_seconds(timestamps: list[datetime]) -> float | None:
    if len(timestamps) < 2:
        return None
    return max((b - a).total_seconds() for a, b in zip(timestamps, timestamps[1:]))


def run_live(stocks: list[Stock]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    actual_process_start = now_tw()
    day = actual_process_start.strftime("%Y-%m-%d")
    day_compact = actual_process_start.strftime("%Y%m%d")
    raw_path = OUTPUT_DIR / f"raw_{day}.ndjson"
    norm_path = OUTPUT_DIR / f"normalized_{day}.parquet"
    csv_path = OUTPUT_DIR / f"normalized_{day}.csv"
    report_path = OUTPUT_DIR / f"report_{day}.json"
    heartbeat_path = OUTPUT_DIR / f"heartbeat_{day}.json"

    arm_at = combine_today(ARM_TIME, actual_process_start)
    start_at = combine_today(START_TIME, actual_process_start)
    startup_deadline_at = combine_today(STARTUP_DEADLINE, actual_process_start)
    first_preopen_deadline_at = combine_today(FIRST_PREOPEN_DEADLINE, actual_process_start)
    last_preopen_earliest_at = combine_today(LAST_PREOPEN_EARLIEST, actual_process_start)
    end_at = combine_today(END_TIME, actual_process_start)

    schedule_delay_seconds = max(0.0, (actual_process_start - arm_at).total_seconds())
    startup_ok = actual_process_start <= startup_deadline_at

    if actual_process_start >= end_at:
        report = {
            "mode": "live",
            "date": day,
            "pass": False,
            "pass_candidate": False,
            "reason": "started_after_end_time",
            "source_gap": True,
            "data_health": "SOURCE_GAP",
            "failure_reasons": ["STARTED_AFTER_END_TIME"],
            "actual_process_start": actual_process_start.isoformat(),
            "schedule_delay_seconds_from_0825": schedule_delay_seconds,
        }
        atomic_write_json(report_path, report)
        print(json.dumps(report, ensure_ascii=False))
        return 3

    while phase_for(now_tw().time()) == "WAIT":
        remaining = (start_at - now_tw()).total_seconds()
        time.sleep(max(0.2, min(5.0, remaining)))

    session = make_session(warmup=True)
    normalized: list[dict[str, Any]] = []
    request_count = 0
    error_count = 0
    empty_count = 0
    stale_row_count = 0
    incomplete_cycle_count = 0
    preopen_rows = 0
    open_rows = 0
    transport_fallback_count = 0
    preopen_valid_cycles: list[datetime] = []
    open_valid_cycles: list[datetime] = []
    all_valid_cycles: list[datetime] = []
    first_capture: str | None = None
    last_capture: str | None = None
    min_observed_symbol_coverage = 1.0
    consecutive_failed_cycles = 0
    max_consecutive_failed_cycles = 0
    requested_codes = {stock.code for stock in stocks}
    minimum_symbols_per_cycle = max(1, math.ceil(len(requested_codes) * MIN_SYMBOL_COVERAGE))

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
            returned_codes_this_cycle: set[str] = set()
            fresh_codes_this_cycle: set[str] = set()
            cycle_error_count = 0

            for batch in chunks(stocks, BATCH_SIZE):
                request_count += 1
                try:
                    raw = fetch_batch(session, batch)
                    transport_mode = raw.get("_qts_transport", {}).get("mode", "batch")
                    if transport_mode != "batch":
                        transport_fallback_count += 1
                    envelope = {
                        "captured_at": captured.isoformat(timespec="milliseconds"),
                        "phase": phase,
                        "request_symbols": [stock.ex_ch for stock in batch],
                        "transport_mode": transport_mode,
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
                        code = str(row.get("code", ""))
                        if code:
                            returned_codes_this_cycle.add(code)
                        if str(row.get("market_date", "")) == day_compact:
                            if code:
                                fresh_codes_this_cycle.add(code)
                        else:
                            stale_row_count += 1

                        if phase == "PREOPEN":
                            preopen_rows += 1
                        elif phase == "OPEN_VALIDATION":
                            open_rows += 1
                except Exception as exc:
                    cycle_error_count += 1
                    error_count += 1
                    raw_file.write(
                        json.dumps(
                            {
                                "captured_at": captured.isoformat(),
                                "phase": phase,
                                "dq_status": "SOURCE_GAP",
                                "error": str(exc),
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
                    raw_file.flush()

            observed_coverage = (
                len(returned_codes_this_cycle & requested_codes) / len(requested_codes)
                if requested_codes
                else 0.0
            )
            min_observed_symbol_coverage = min(min_observed_symbol_coverage, observed_coverage)
            cycle_valid = len(fresh_codes_this_cycle & requested_codes) >= minimum_symbols_per_cycle

            if cycle_valid:
                all_valid_cycles.append(captured)
                if first_capture is None:
                    first_capture = captured.isoformat()
                last_capture = captured.isoformat()
                if phase == "PREOPEN":
                    preopen_valid_cycles.append(captured)
                elif phase == "OPEN_VALIDATION":
                    open_valid_cycles.append(captured)
            else:
                incomplete_cycle_count += 1

            if cycle_error_count > 0 or not cycle_valid:
                consecutive_failed_cycles += 1
            else:
                consecutive_failed_cycles = 0
            max_consecutive_failed_cycles = max(max_consecutive_failed_cycles, consecutive_failed_cycles)
            source_gap_active = consecutive_failed_cycles >= SOURCE_GAP_CONSECUTIVE_CYCLES

            atomic_write_json(
                heartbeat_path,
                {
                    "date": day,
                    "captured_at": captured.isoformat(),
                    "phase": phase,
                    "valid_cycle": cycle_valid,
                    "data_health": "SOURCE_GAP" if source_gap_active else "NORMAL",
                    "source_gap": source_gap_active,
                    "consecutive_failed_cycles": consecutive_failed_cycles,
                    "max_consecutive_failed_cycles": max_consecutive_failed_cycles,
                    "transport_fallback_count": transport_fallback_count,
                    "returned_symbols": len(returned_codes_this_cycle & requested_codes),
                    "fresh_symbols": len(fresh_codes_this_cycle & requested_codes),
                    "requested_symbols": len(requested_codes),
                    "request_count": request_count,
                    "error_count": error_count,
                    "stale_row_count": stale_row_count,
                },
            )

            next_poll += POLL_SECONDS
            if next_poll < time.monotonic() - POLL_SECONDS:
                next_poll = time.monotonic() + POLL_SECONDS

    if normalized:
        df = pd.DataFrame(normalized)
        df.to_parquet(norm_path, index=False)
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    expected_preopen = expected_cycles(START_TIME, PREOPEN_END, actual_process_start)
    expected_open = expected_cycles(PREOPEN_END, END_TIME, actual_process_start)
    preopen_coverage = len(preopen_valid_cycles) / expected_preopen if expected_preopen else 0.0
    open_coverage = len(open_valid_cycles) / expected_open if expected_open else 0.0
    observed_max_gap = max_gap_seconds(all_valid_cycles)

    first_preopen = preopen_valid_cycles[0] if preopen_valid_cycles else None
    last_preopen = preopen_valid_cycles[-1] if preopen_valid_cycles else None
    first_preopen_ok = bool(first_preopen and first_preopen <= first_preopen_deadline_at)
    last_preopen_ok = bool(last_preopen and last_preopen >= last_preopen_earliest_at)
    preopen_coverage_ok = preopen_coverage >= MIN_PREOPEN_COVERAGE
    open_coverage_ok = open_coverage >= MIN_OPEN_COVERAGE
    gap_ok = observed_max_gap is not None and observed_max_gap <= MAX_ALLOWED_GAP_SECONDS
    freshness_ok = stale_row_count == 0
    symbol_coverage_ok = min_observed_symbol_coverage >= MIN_SYMBOL_COVERAGE
    request_error_rate = error_count / request_count if request_count else 1.0
    error_rate_ok = request_error_rate <= MAX_REQUEST_ERROR_RATE
    source_gap_ok = max_consecutive_failed_cycles < SOURCE_GAP_CONSECUTIVE_CYCLES

    gates = {
        "startup_before_082945": startup_ok,
        "first_preopen_by_083020": first_preopen_ok,
        "last_preopen_at_or_after_085940": last_preopen_ok,
        "preopen_coverage_ok": preopen_coverage_ok,
        "open_validation_coverage_ok": open_coverage_ok,
        "max_gap_ok": gap_ok,
        "request_error_rate_ok": error_rate_ok,
        "source_gap_not_persistent": source_gap_ok,
        "fresh_market_date_only": freshness_ok,
        "symbol_coverage_ok": symbol_coverage_ok,
    }
    failure_reasons = [name.upper() for name, passed in gates.items() if not passed]
    pass_candidate = all(gates.values())

    report = {
        "mode": "live",
        "date": day,
        "actual_process_start": actual_process_start.isoformat(),
        "first_capture": first_capture,
        "last_capture": last_capture,
        "first_preopen_valid_capture": first_preopen.isoformat() if first_preopen else None,
        "last_preopen_valid_capture": last_preopen.isoformat() if last_preopen else None,
        "schedule_delay_seconds_from_0825": schedule_delay_seconds,
        "poll_seconds": POLL_SECONDS,
        "requested_symbols": len(stocks),
        "requests": request_count,
        "normalized_rows": len(normalized),
        "preopen_rows": preopen_rows,
        "open_validation_rows": open_rows,
        "error_count": error_count,
        "request_error_rate": request_error_rate,
        "allowed_request_error_rate": MAX_REQUEST_ERROR_RATE,
        "transport_fallback_count": transport_fallback_count,
        "empty_response_count": empty_count,
        "stale_row_count": stale_row_count,
        "incomplete_cycle_count": incomplete_cycle_count,
        "max_consecutive_failed_cycles": max_consecutive_failed_cycles,
        "source_gap_threshold_cycles": SOURCE_GAP_CONSECUTIVE_CYCLES,
        "source_gap": not source_gap_ok,
        "data_health": "NORMAL" if source_gap_ok else "SOURCE_GAP",
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
        "gates": gates,
        "failure_reasons": failure_reasons,
        "pass_candidate": pass_candidate,
        "pass": pass_candidate,
        "raw_file": str(raw_path),
        "normalized_parquet": str(norm_path),
        "normalized_csv": str(csv_path),
        "heartbeat_file": str(heartbeat_path),
    }
    atomic_write_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False))
    return 0 if pass_candidate else 4


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
                "at": now_tw().isoformat(),
                "error": repr(exc),
                "run_mode": RUN_MODE,
                "source_gap": True,
                "data_health": "SOURCE_GAP",
            },
        )
        print(f"FATAL: {exc}", file=sys.stderr)
        sys.exit(99)
