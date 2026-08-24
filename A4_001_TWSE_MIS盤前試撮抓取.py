from __future__ import annotations

import csv
import json
import math
import os
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import pandas as pd
import requests

VERSION = "3.0.0"
TZ = ZoneInfo("Asia/Taipei")
MIS_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"

RUN_MODE = os.getenv("RUN_MODE", "live").strip().lower()
STOCKS_FILE = Path(os.getenv("STOCKS_FILE", "stocks.csv"))
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output/mis"))

POLL_SECONDS = max(10.0, float(os.getenv("POLL_SECONDS", "30")))
START_TIME = dtime.fromisoformat(os.getenv("START_TIME", "08:30:00"))
PREOPEN_END = dtime.fromisoformat(os.getenv("PREOPEN_END", "09:00:00"))
END_TIME = dtime.fromisoformat(os.getenv("END_TIME", "09:30:00"))

TARGET_BATCH_SYMBOLS = max(10, int(os.getenv("TARGET_BATCH_SYMBOLS", "90")))
MIN_BATCH_SYMBOLS = max(1, int(os.getenv("MIN_BATCH_SYMBOLS", "8")))
MAX_URL_CHARS = max(900, int(os.getenv("MAX_URL_CHARS", "1800")))
MIN_REQUEST_INTERVAL = max(0.05, float(os.getenv("MIN_REQUEST_INTERVAL", "0.25")))
REQUEST_TIMEOUT = max(1.0, float(os.getenv("REQUEST_TIMEOUT", "3.0")))
REQUEST_RETRIES = max(0, int(os.getenv("REQUEST_RETRIES", "2")))
CYCLE_BUDGET_SECONDS = max(5.0, float(os.getenv("CYCLE_BUDGET_SECONDS", "20.0")))
CYCLE_GUARD_SECONDS = max(0.10, float(os.getenv("CYCLE_GUARD_SECONDS", "0.50")))
BACKOFF_BASE_SECONDS = max(0.20, float(os.getenv("BACKOFF_BASE_SECONDS", "0.60")))
BACKOFF_CAP_SECONDS = max(BACKOFF_BASE_SECONDS, float(os.getenv("BACKOFF_CAP_SECONDS", "4.0")))
MIN_SYMBOL_COVERAGE = min(1.0, max(0.0, float(os.getenv("MIN_SYMBOL_COVERAGE", "0.995"))))
MAX_TERMINAL_ERROR_RATE = min(1.0, max(0.0, float(os.getenv("MAX_TERMINAL_ERROR_RATE", "0.02"))))
SOURCE_GAP_CONSECUTIVE_CYCLES = max(1, int(os.getenv("SOURCE_GAP_CONSECUTIVE_CYCLES", "3")))
NO_QUOTE_CONFIRMATIONS = max(2, int(os.getenv("NO_QUOTE_CONFIRMATIONS", "3")))
NO_QUOTE_PROBE_LIMIT = max(0, int(os.getenv("NO_QUOTE_PROBE_LIMIT", "30")))

RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
SIZE_HTTP = {400, 414, 422}

@dataclass(frozen=True)
class Stock:
    market: str
    code: str
    ex_ch: str

@dataclass
class BatchResult:
    items: list[dict[str, Any]]
    requested_codes: list[str]
    ok: bool
    terminal: bool
    error_type: str | None
    error: str | None
    http_status: int | None
    split_depth: int

def now_tw() -> datetime:
    return datetime.now(TZ)

def combine_today(t: dtime, base: datetime | None = None) -> datetime:
    base = base or now_tw()
    return datetime.combine(base.date(), t, TZ)

def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)

def load_stocks(path: Path) -> list[Stock]:
    if not path.exists():
        raise FileNotFoundError(f"stocks file not found: {path}")
    out: list[Stock] = []
    seen: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        if not r.fieldnames or "code" not in r.fieldnames:
            raise ValueError("stocks.csv must contain code column")
        for row in r:
            code = str(row.get("code", "")).strip()
            market = str(row.get("market", "TSE")).strip().upper()
            if not code or market not in {"TSE", "OTC"}:
                continue
            key = (market, code)
            if key in seen:
                continue
            seen.add(key)
            ex_ch = str(row.get("ex_ch", "")).strip()
            if not ex_ch:
                ex_ch = f"{'tse' if market == 'TSE' else 'otc'}_{code}.tw"
            out.append(Stock(market, code, ex_ch))
    if not out:
        raise RuntimeError("stocks file contains no valid symbols")
    return out

def make_session() -> requests.Session:
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0, pool_block=True)
    s.mount("https://", adapter)
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) QTS-A4-Adaptive/3.0",
        "Accept": "application/json,text/plain,*/*",
        "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.7",
        "Referer": "https://mis.twse.com.tw/stock/fibest.jsp",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Connection": "keep-alive",
    })
    return s

def prepare_url_len(batch: list[Stock]) -> int:
    params = {"ex_ch": "|".join(x.ex_ch for x in batch), "json": "1", "delay": "0", "_": "0000000000000"}
    return len(MIS_URL) + 1 + len(urlencode(params))

def build_batches(stocks: list[Stock], max_symbols: int) -> list[list[Stock]]:
    batches: list[list[Stock]] = []
    cur: list[Stock] = []
    for stock in stocks:
        candidate = cur + [stock]
        if cur and (len(candidate) > max_symbols or prepare_url_len(candidate) > MAX_URL_CHARS):
            batches.append(cur)
            cur = [stock]
        else:
            cur = candidate
    if cur:
        batches.append(cur)
    return batches

def classify_exception(exc: Exception) -> tuple[str, int | None]:
    if isinstance(exc, requests.Timeout):
        return "TIMEOUT", None
    if isinstance(exc, requests.ConnectionError):
        return "CONNECTION_ERROR", None
    if isinstance(exc, requests.HTTPError):
        status = exc.response.status_code if exc.response is not None else None
        return f"HTTP_{status}" if status else "HTTP_ERROR", status
    return type(exc).__name__.upper(), None

class AdaptiveTransport:
    def __init__(self) -> None:
        self.session = make_session()
        self.last_request_mono = 0.0
        self.safe_batch_symbols = TARGET_BATCH_SYMBOLS
        self.stats = Counter()
        self.sample_errors: list[dict[str, Any]] = []
        self.no_quote_counts: Counter[str] = Counter()
        self.no_quote_codes: set[str] = set()

    def close(self) -> None:
        self.session.close()

    def reset_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = make_session()
        self.stats["session_resets"] += 1

    def pace(self, deadline: float) -> bool:
        wait = MIN_REQUEST_INTERVAL - (time.monotonic() - self.last_request_mono)
        if wait <= 0:
            return True
        if time.monotonic() + wait >= deadline - CYCLE_GUARD_SECONDS:
            return False
        time.sleep(wait)
        return True

    def _request(self, batch: list[Stock], deadline: float) -> tuple[dict[str, Any] | None, str | None, int | None, str | None]:
        if not self.pace(deadline):
            return None, "CYCLE_DEADLINE", None, "pacing would exceed cycle deadline"
        remaining = deadline - time.monotonic() - CYCLE_GUARD_SECONDS
        if remaining <= 0:
            return None, "CYCLE_DEADLINE", None, "cycle deadline exhausted"
        params = {"ex_ch": "|".join(x.ex_ch for x in batch), "json": "1", "delay": "0", "_": str(int(time.time() * 1000))}
        self.stats["http_attempts"] += 1
        self.last_request_mono = time.monotonic()
        try:
            r = self.session.get(MIS_URL, params=params, timeout=min(REQUEST_TIMEOUT, max(0.5, remaining)))
            status = r.status_code
            if status in SIZE_HTTP:
                self.stats["size_rejections"] += 1
                return None, f"HTTP_{status}", status, f"HTTP {status}"
            if status in RETRYABLE_HTTP:
                self.stats["retryable_http"] += 1
                return None, f"HTTP_{status}", status, f"HTTP {status}"
            r.raise_for_status()
            obj = r.json()
            if not isinstance(obj, dict):
                return None, "MIS_SCHEMA_ERROR", status, "response is not object"
            rtcode = str(obj.get("rtcode", ""))
            if rtcode != "0000":
                self.stats["mis_rtcode_errors"] += 1
                return obj, "MIS_RTCODE", status, f"rtcode={rtcode} message={obj.get('rtmessage')}"
            if not isinstance(obj.get("msgArray", []), list):
                return obj, "MIS_SCHEMA_ERROR", status, "msgArray is not list"
            return obj, None, status, None
        except Exception as exc:
            et, status = classify_exception(exc)
            if et in {"CONNECTION_ERROR", "TIMEOUT"}:
                self.stats["transport_exceptions"] += 1
            return None, et, status, f"{type(exc).__name__}: {exc}"

    def warmup(self) -> dict[str, Any]:
        probes = [Stock("TSE", "2330", "tse_2330.tw"), Stock("OTC", "6488", "otc_6488.tw")]
        deadline = time.monotonic() + 10.0
        errors: list[str] = []
        for attempt in range(1, 4):
            obj, et, status, err = self._request(probes, deadline)
            if et is None and isinstance(obj, dict):
                items = obj.get("msgArray", []) or []
                codes = {str(x.get("c", "")).strip() for x in items if isinstance(x, dict)}
                if codes & {"2330", "6488"}:
                    return {"ok": True, "attempt": attempt, "status": status, "returned_probe_symbols": sorted(codes & {"2330", "6488"})}
            errors.append(f"{et}:{err}")
            self.reset_session()
            time.sleep(min(2.0, 0.4 * attempt))
        return {"ok": False, "errors": errors[-5:]}

    def _record_error(self, batch: list[Stock], et: str | None, status: int | None, err: str | None, depth: int) -> None:
        if len(self.sample_errors) >= 12:
            return
        self.sample_errors.append({"batch_size": len(batch), "url_chars": prepare_url_len(batch), "error_type": et, "http_status": status, "error": (err or "")[:300], "split_depth": depth})

    def fetch_batch(self, batch: list[Stock], deadline: float, depth: int = 0) -> list[BatchResult]:
        if not batch:
            return []
        last_et = None
        last_status = None
        last_err = None
        for attempt in range(REQUEST_RETRIES + 1):
            obj, et, status, err = self._request(batch, deadline)
            if et is None and isinstance(obj, dict):
                items = [x for x in (obj.get("msgArray", []) or []) if isinstance(x, dict)]
                self.stats["successful_batches"] += 1
                return [BatchResult(items, [x.code for x in batch], True, False, None, None, status, depth)]
            last_et, last_status, last_err = et, status, err
            if et in {"HTTP_400", "HTTP_414", "HTTP_422", "MIS_RTCODE"} and len(batch) > MIN_BATCH_SYMBOLS:
                self.stats["adaptive_splits"] += 1
                self._record_error(batch, et, status, err, depth)
                self.safe_batch_symbols = min(self.safe_batch_symbols, max(MIN_BATCH_SYMBOLS, len(batch) // 2))
                mid = len(batch) // 2
                return self.fetch_batch(batch[:mid], deadline, depth + 1) + self.fetch_batch(batch[mid:], deadline, depth + 1)
            if et in {"CONNECTION_ERROR", "TIMEOUT", "HTTP_429", "HTTP_500", "HTTP_502", "HTTP_503", "HTTP_504"} and attempt < REQUEST_RETRIES:
                self.stats["transport_retries"] += 1
                self.reset_session()
                delay = min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** attempt))
                if time.monotonic() + delay < deadline - CYCLE_GUARD_SECONDS:
                    time.sleep(delay)
                    continue
            break
        self.stats["terminal_batch_errors"] += 1
        self._record_error(batch, last_et, last_status, last_err, depth)
        return [BatchResult([], [x.code for x in batch], False, True, last_et, last_err, last_status, depth)]

    def probe_missing(self, missing: list[Stock], deadline: float) -> list[dict[str, Any]]:
        recovered: list[dict[str, Any]] = []
        for stock in missing[:NO_QUOTE_PROBE_LIMIT]:
            if time.monotonic() >= deadline - CYCLE_GUARD_SECONDS:
                break
            obj, et, status, err = self._request([stock], deadline)
            if et is None and isinstance(obj, dict):
                items = [x for x in (obj.get("msgArray", []) or []) if isinstance(x, dict)]
                exact = [x for x in items if str(x.get("c", "")).strip() == stock.code]
                if exact:
                    recovered.extend(exact)
                    self.no_quote_counts[stock.code] = 0
                else:
                    self.no_quote_counts[stock.code] += 1
                    if self.no_quote_counts[stock.code] >= NO_QUOTE_CONFIRMATIONS:
                        self.no_quote_codes.add(stock.code)
            else:
                self._record_error([stock], et, status, err, 99)
        return recovered

    def capture_cycle(self, stocks: list[Stock], deadline: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        started = time.monotonic()
        self.sample_errors = []
        stats_before = self.stats.copy()
        batches = build_batches(stocks, self.safe_batch_symbols)
        by_code = {s.code: s for s in stocks}
        best: dict[str, dict[str, Any]] = {}
        terminal_codes: set[str] = set()
        for batch in batches:
            if time.monotonic() >= deadline - CYCLE_GUARD_SECONDS:
                terminal_codes.update(x.code for x in batch)
                self.stats["deadline_batches"] += 1
                continue
            for result in self.fetch_batch(batch, deadline):
                if result.terminal:
                    terminal_codes.update(result.requested_codes)
                for item in result.items:
                    code = str(item.get("c", "")).strip()
                    if code in by_code:
                        best[code] = item
        missing_codes = sorted(set(by_code) - set(best))
        if missing_codes and NO_QUOTE_PROBE_LIMIT > 0:
            recovered = self.probe_missing([by_code[c] for c in missing_codes], deadline)
            for item in recovered:
                code = str(item.get("c", "")).strip()
                if code in by_code:
                    best[code] = item
        all_codes = set(by_code)
        active_expected = all_codes - self.no_quote_codes
        returned = set(best)
        active_returned = returned & active_expected
        raw_coverage = len(returned) / len(all_codes) if all_codes else 0.0
        active_coverage = len(active_returned) / len(active_expected) if active_expected else 1.0
        delta = self.stats - stats_before
        attempts = int(delta["http_attempts"])
        terminal_batch_errors = int(delta["terminal_batch_errors"])
        terminal_error_rate = terminal_batch_errors / max(1, len(batches))
        diag = {
            "version": VERSION,
            "requested_symbols": len(stocks),
            "returned_symbols": len(returned),
            "active_expected_symbols": len(active_expected),
            "raw_coverage": raw_coverage,
            "active_coverage": active_coverage,
            "request_attempts": attempts,
            "terminal_batch_errors": terminal_batch_errors,
            "terminal_error_rate": terminal_error_rate,
            "successful_batches": int(delta["successful_batches"]),
            "adaptive_splits": int(delta["adaptive_splits"]),
            "transport_retries": int(delta["transport_retries"]),
            "session_resets": int(delta["session_resets"]),
            "size_rejections": int(delta["size_rejections"]),
            "mis_rtcode_errors": int(delta["mis_rtcode_errors"]),
            "transport_exceptions": int(delta["transport_exceptions"]),
            "deadline_batches": int(delta["deadline_batches"]),
            "initial_batches": len(batches),
            "safe_batch_symbols": self.safe_batch_symbols,
            "max_url_chars": MAX_URL_CHARS,
            "min_request_interval": MIN_REQUEST_INTERVAL,
            "missing_symbols": sorted(all_codes - returned),
            "no_quote_symbols": sorted(self.no_quote_codes),
            "terminal_codes": sorted(terminal_codes),
            "sample_errors": list(self.sample_errors),
            "cycle_elapsed_seconds": time.monotonic() - started,
            "cycle_budget_seconds": CYCLE_BUDGET_SECONDS,
        }
        return [best[c] for c in sorted(best)], diag

def publish_github_guard(state: str, description: str) -> None:
    token = os.getenv("GITHUB_TOKEN", "").strip()
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    sha = os.getenv("GITHUB_SHA", "").strip()
    if not token or not repo or not sha:
        return
    try:
        requests.post(
            f"https://api.github.com/repos/{repo}/statuses/{sha}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "QTS-A4-Adaptive/3.0"},
            json={"state": state, "context": "qts/a4/runtime-guard", "description": description[:140]},
            timeout=5,
        )
    except Exception:
        return

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

def normalize_item(item: dict[str, Any], captured_at: datetime, phase: str) -> dict[str, Any]:
    bp = parse_levels(item.get("b"), float)
    ap = parse_levels(item.get("a"), float)
    bv = parse_levels(item.get("g"), int)
    av = parse_levels(item.get("f"), int)
    def lv(xs: list[Any], i: int) -> Any:
        return xs[i] if len(xs) > i else None
    row = {"captured_at": captured_at.isoformat(timespec="milliseconds"), "phase": phase, "market": str(item.get("ex", "")), "code": str(item.get("c", "")).strip(), "name": str(item.get("n", "")), "market_date": str(item.get("d", "")).strip(), "market_time": str(item.get("t", "")).strip(), "exchange_time": str(item.get("%", "")).strip(), "simulated_flag": str(item.get("ts", "")), "last_price": item.get("z"), "single_volume": item.get("tv"), "total_volume": item.get("v"), "reference_price": item.get("y"), "open": item.get("o"), "high": item.get("h"), "low": item.get("l"), "raw_pz": item.get("pz"), "raw_ps": item.get("ps"), "raw_oa": item.get("oa"), "raw_ob": item.get("ob"), "raw_ip": item.get("ip")}
    for i in range(5):
        row[f"bid_price_{i+1}"] = lv(bp, i)
        row[f"bid_volume_{i+1}"] = lv(bv, i)
        row[f"ask_price_{i+1}"] = lv(ap, i)
        row[f"ask_volume_{i+1}"] = lv(av, i)
    return row

def phase_for(t: dtime) -> str:
    if t < START_TIME:
        return "WAIT"
    if t < PREOPEN_END:
        return "PREOPEN"
    if t < END_TIME:
        return "OPEN_VALIDATION"
    return "DONE"

def run_env_test(stocks: list[Stock]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    transport = AdaptiveTransport()
    started = now_tw()
    try:
        warm = transport.warmup()
        deadline = time.monotonic() + CYCLE_BUDGET_SECONDS
        items, diag = transport.capture_cycle(stocks, deadline)
        rows = [normalize_item(x, now_tw(), "ENV_TEST") for x in items]
        coverage = float(diag["active_coverage"])
        terminal_error_rate = float(diag["terminal_error_rate"])
        passed = bool(warm.get("ok")) and coverage >= MIN_SYMBOL_COVERAGE and terminal_error_rate <= MAX_TERMINAL_ERROR_RATE and float(diag["cycle_elapsed_seconds"]) <= CYCLE_BUDGET_SECONDS + 0.5
        report = {"version": VERSION, "mode": "env_test", "started_at": started.isoformat(), "finished_at": now_tw().isoformat(), "requested_symbols": len(stocks), "returned_symbols": diag["returned_symbols"], "active_expected_symbols": diag["active_expected_symbols"], "coverage": coverage, "raw_coverage": diag["raw_coverage"], "missing_symbols": diag["missing_symbols"], "no_quote_symbols": diag["no_quote_symbols"], "warmup_ok_count": 1 if warm.get("ok") else 0, "warmup_total": 1, "warmup": [warm], "request_attempts": diag["request_attempts"], "request_errors": diag["terminal_batch_errors"], "request_error_rate": terminal_error_rate, "terminal_error_rate": terminal_error_rate, "adaptive_splits": diag["adaptive_splits"], "safe_batch_symbols": diag["safe_batch_symbols"], "sample_errors": diag["sample_errors"], "cycle_elapsed_seconds": diag["cycle_elapsed_seconds"], "cycle_budget_seconds": CYCLE_BUDGET_SECONDS, "pass": passed}
        atomic_json(OUTPUT_DIR / "env_test_report.json", report)
        atomic_json(OUTPUT_DIR / "env_test_diagnostics.json", diag)
        atomic_json(OUTPUT_DIR / "transport_profile.json", {"version": VERSION, "safe_batch_symbols": transport.safe_batch_symbols, "max_url_chars": MAX_URL_CHARS, "min_request_interval": MIN_REQUEST_INTERVAL, "no_quote_symbols": sorted(transport.no_quote_codes), "updated_at": now_tw().isoformat()})
        if rows:
            pd.DataFrame(rows).to_csv(OUTPUT_DIR / "env_test_normalized.csv", index=False, encoding="utf-8-sig")
        print(json.dumps(report, ensure_ascii=False))
        return 0 if passed else 2
    finally:
        transport.close()

def run_live(stocks: list[Stock]) -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    process_start = now_tw()
    day = process_start.strftime("%Y-%m-%d")
    day_compact = process_start.strftime("%Y%m%d")
    start_at = combine_today(START_TIME, process_start)
    end_at = combine_today(END_TIME, process_start)
    raw_path = OUTPUT_DIR / f"raw_{day}.ndjson"
    csv_path = OUTPUT_DIR / f"normalized_{day}.csv"
    pq_path = OUTPUT_DIR / f"normalized_{day}.parquet"
    report_path = OUTPUT_DIR / f"report_{day}.json"
    heartbeat_path = OUTPUT_DIR / f"heartbeat_{day}.json"
    profile_path = OUTPUT_DIR / f"transport_profile_{day}.json"
    if process_start >= end_at:
        rep = {"version": VERSION, "mode": "live", "date": day, "pass": False, "failure_reasons": ["STARTED_AFTER_END_TIME"]}
        atomic_json(report_path, rep)
        print(json.dumps(rep, ensure_ascii=False))
        return 3
    transport = AdaptiveTransport()
    warm = transport.warmup()
    if not warm.get("ok"):
        rep = {"version": VERSION, "mode": "live", "date": day, "pass": False, "failure_reasons": ["WARMUP_FAIL"], "warmup": warm}
        atomic_json(report_path, rep)
        return 4
    while now_tw() < start_at:
        time.sleep(min(1.0, max(0.05, (start_at - now_tw()).total_seconds())))
    requested_codes = {s.code for s in stocks}
    expected_market = {s.code: ("tse" if s.market == "TSE" else "otc") for s in stocks}
    total_cycles = valid_cycles = preopen_cycles = preopen_valid = open_cycles = open_valid = 0
    request_attempts = terminal_errors = incomplete_cycles = 0
    consecutive_bad = max_consecutive_bad = 0
    coverage_values: list[float] = []
    first_capture = last_capture = None
    failure_reasons: list[str] = []
    header_written = csv_path.exists() and csv_path.stat().st_size > 0
    with raw_path.open("a", encoding="utf-8") as raw_f, csv_path.open("a", encoding="utf-8-sig", newline="") as csv_f:
        writer: csv.DictWriter | None = None
        cycle_index = max(0, int((now_tw() - start_at).total_seconds() // POLL_SECONDS))
        while True:
            target = start_at + timedelta(seconds=cycle_index * POLL_SECONDS)
            if target >= end_at:
                break
            current = now_tw()
            if current < target:
                time.sleep(min(0.25, (target - current).total_seconds()))
                continue
            captured = now_tw()
            phase = phase_for(captured.time())
            if phase == "DONE":
                break
            total_cycles += 1
            if phase == "PREOPEN": preopen_cycles += 1
            elif phase == "OPEN_VALIDATION": open_cycles += 1
            deadline = time.monotonic() + CYCLE_BUDGET_SECONDS
            items, diag = transport.capture_cycle(stocks, deadline)
            request_attempts += int(diag["request_attempts"])
            terminal_errors += int(diag["terminal_batch_errors"])
            accepted: list[dict[str, Any]] = []
            seen: set[str] = set()
            integrity = Counter()
            for item in items:
                code = str(item.get("c", "")).strip()
                market = str(item.get("ex", "")).strip().lower()
                mdate = str(item.get("d", "")).strip()
                if not code or not market or not mdate: integrity["schema_missing"] += 1; continue
                if code not in requested_codes: integrity["unexpected_code"] += 1; continue
                if expected_market[code] != market: integrity["market_mismatch"] += 1; continue
                if mdate != day_compact: integrity["stale_date"] += 1; continue
                if code in seen: integrity["duplicate_code"] += 1; continue
                seen.add(code); accepted.append(item)
            active_expected = requested_codes - set(diag["no_quote_symbols"])
            active_coverage = len(seen & active_expected) / len(active_expected) if active_expected else 1.0
            coverage_values.append(active_coverage)
            cycle_ok = active_coverage >= MIN_SYMBOL_COVERAGE and not any(integrity.values()) and float(diag["terminal_error_rate"]) <= MAX_TERMINAL_ERROR_RATE
            if cycle_ok:
                valid_cycles += 1
                if phase == "PREOPEN": preopen_valid += 1
                elif phase == "OPEN_VALIDATION": open_valid += 1
                consecutive_bad = 0
                first_capture = first_capture or captured.isoformat()
                last_capture = captured.isoformat()
            else:
                incomplete_cycles += 1
                consecutive_bad += 1
                max_consecutive_bad = max(max_consecutive_bad, consecutive_bad)
            rows = [normalize_item(x, captured, phase) for x in accepted]
            if rows:
                if writer is None:
                    writer = csv.DictWriter(csv_f, fieldnames=list(rows[0].keys()))
                    if not header_written: writer.writeheader(); header_written = True
                writer.writerows(rows); csv_f.flush()
            raw_f.write(json.dumps({"captured_at": captured.isoformat(timespec="milliseconds"), "phase": phase, "cycle_index": cycle_index, "target_at": target.isoformat(), "diagnostics": {**diag, "integrity": dict(integrity), "cycle_ok": cycle_ok, "effective_coverage": active_coverage}, "items": accepted}, ensure_ascii=False) + "\n")
            raw_f.flush()
            atomic_json(heartbeat_path, {"version": VERSION, "date": day, "captured_at": captured.isoformat(), "phase": phase, "cycle_index": cycle_index, "valid_cycle": cycle_ok, "requested_symbols": len(requested_codes), "returned_symbols": len(seen), "active_expected_symbols": len(active_expected), "active_coverage": active_coverage, "raw_coverage": diag["raw_coverage"], "terminal_error_rate": diag["terminal_error_rate"], "safe_batch_symbols": transport.safe_batch_symbols, "adaptive_splits": diag["adaptive_splits"], "no_quote_symbols": diag["no_quote_symbols"], "integrity": dict(integrity)})
            atomic_json(profile_path, {"version": VERSION, "safe_batch_symbols": transport.safe_batch_symbols, "max_url_chars": MAX_URL_CHARS, "min_request_interval": MIN_REQUEST_INTERVAL, "no_quote_symbols": sorted(transport.no_quote_codes), "updated_at": captured.isoformat()})
            publish_github_guard("success" if cycle_ok else "failure", f"{phase} cov={active_coverage:.4f} err={float(diag['terminal_error_rate']):.3f} batch={transport.safe_batch_symbols}")
            if consecutive_bad >= SOURCE_GAP_CONSECUTIVE_CYCLES:
                transport.reset_session(); time.sleep(min(2.0, BACKOFF_BASE_SECONDS))
            cycle_index += 1
    if csv_path.exists() and csv_path.stat().st_size > 0:
        try:
            pd.read_csv(csv_path, dtype={"code": str, "market_date": str}).to_parquet(pq_path, index=False)
        except Exception:
            pass
    request_error_rate = terminal_errors / max(1, request_attempts)
    pre_ratio = preopen_valid / max(1, preopen_cycles)
    open_ratio = open_valid / max(1, open_cycles)
    p05 = None
    if coverage_values:
        xs = sorted(coverage_values); p = (len(xs) - 1) * 0.05; lo, hi = math.floor(p), math.ceil(p)
        p05 = xs[lo] if lo == hi else xs[lo] * (hi - p) + xs[hi] * (p - lo)
    passed = total_cycles > 0 and p05 is not None and p05 >= MIN_SYMBOL_COVERAGE and pre_ratio >= 0.95 and open_ratio >= 0.90 and max_consecutive_bad <= 2 and request_error_rate <= MAX_TERMINAL_ERROR_RATE
    if p05 is None or p05 < MIN_SYMBOL_COVERAGE: failure_reasons.append("P05_COVERAGE")
    if pre_ratio < 0.95: failure_reasons.append("PREOPEN_RATIO")
    if open_ratio < 0.90: failure_reasons.append("OPEN_RATIO")
    if max_consecutive_bad > 2: failure_reasons.append("MAX_BAD_STREAK")
    if request_error_rate > MAX_TERMINAL_ERROR_RATE: failure_reasons.append("REQUEST_ERROR_RATE")
    report = {"version": VERSION, "mode": "live", "date": day, "pass": passed, "source_gap": not passed, "data_health": "PASS" if passed else "SOURCE_GAP", "failure_reasons": failure_reasons, "actual_process_start": process_start.isoformat(), "first_capture": first_capture, "last_capture": last_capture, "requested_symbols": len(requested_codes), "total_cycles": total_cycles, "valid_cycles": valid_cycles, "preopen_cycles": preopen_cycles, "preopen_valid_cycles": preopen_valid, "open_cycles": open_cycles, "open_valid_cycles": open_valid, "preopen_valid_ratio": pre_ratio, "open_valid_ratio": open_ratio, "p05_coverage": p05, "incomplete_cycle_count": incomplete_cycles, "max_consecutive_failed_cycles": max_consecutive_bad, "request_attempts": request_attempts, "request_errors": terminal_errors, "request_error_rate": request_error_rate, "safe_batch_symbols": transport.safe_batch_symbols, "no_quote_symbols": sorted(transport.no_quote_codes), "warmup": warm, "raw_path": str(raw_path), "csv_path": str(csv_path), "parquet_path": str(pq_path)}
    atomic_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False))
    transport.close()
    return 0 if passed else 5

def main() -> int:
    stocks = load_stocks(STOCKS_FILE)
    if RUN_MODE == "env_test": return run_env_test(stocks)
    if RUN_MODE == "live": return run_live(stocks)
    raise ValueError(f"unsupported RUN_MODE={RUN_MODE}")

if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        atomic_json(OUTPUT_DIR / "fatal_error.json", {"version": VERSION, "mode": RUN_MODE, "error": f"{type(exc).__name__}: {exc}", "checked_at": now_tw().isoformat()})
        print(f"A4_ENGINE_FATAL:{type(exc).__name__}:{exc}", file=sys.stderr)
        raise SystemExit(99)
