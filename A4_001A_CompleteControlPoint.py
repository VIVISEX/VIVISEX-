from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import html
import json
import math
import os
import re
import subprocess
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dtime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

VERSION = "A4_001A_COMPLETE_CONTROL_POINT_V2.1.0"
COMPONENT = "A4_001A"
TZ = ZoneInfo("Asia/Taipei")
ENGINE = Path(__file__).resolve().parent / "A4_001_TWSE_MIS盤前試撮抓取.py"
OUT = Path(os.getenv("OUTPUT_DIR", "output/a4"))
STATUS = Path(os.getenv("A4_STATUS_DIR", "status/a4"))
CHECKPOINT = OUT / "checkpoint.json"
TWSE_COMPANY = "https://openapi.twse.com.tw/v1/opendata/t187ap03_L"
TPEX_COMPANY = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O"
TWSE_HOLIDAY = "https://openapi.twse.com.tw/v1/holidaySchedule/holidaySchedule"
DGPA = "https://www.dgpa.gov.tw/typh/daily/nds.html"
UA = "Mozilla/5.0 QTS-A4-Complete-Control-Point/2.1"
START = dtime(8, 30)
PREOPEN_END = dtime(9, 0)
END = dtime(9, 30)
FINAL_GATE = dtime(8, 28, 30)
MIN_TWSE = int(os.getenv("MIN_TWSE_SYMBOLS", "800"))
MIN_TPEX = int(os.getenv("MIN_TPEX_SYMBOLS", "600"))
MIN_TOTAL = int(os.getenv("MIN_TOTAL_SYMBOLS", "1500"))
MAX_TOTAL = int(os.getenv("MAX_TOTAL_SYMBOLS", "3000"))
POLL = float(os.getenv("POLL_SECONDS", "10"))
PER_CYCLE = float(os.getenv("PER_CYCLE_FRESH_COVERAGE", "0.97"))
PRE_RATIO = float(os.getenv("MIN_PREOPEN_VALID_CYCLES", "0.95"))
OPEN_RATIO = float(os.getenv("MIN_OPEN_VALID_CYCLES", "0.90"))
P05_MIN = float(os.getenv("MIN_P05_SYMBOL_COVERAGE", "0.95"))
MAX_BAD_STREAK = int(os.getenv("MAX_CONSECUTIVE_INVALID_CYCLES", "2"))
MAX_REQ_ERR = float(os.getenv("MAX_REQUEST_ERROR_RATE", "0.05"))
MAX_GAP = float(os.getenv("MAX_ALLOWED_GAP_SECONDS", "30"))


@dataclass(frozen=True)
class Stock:
    market: str
    code: str
    name: str
    ex_ch: str


class Text(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data.strip())


def now() -> datetime:
    return datetime.now(TZ)


def iso() -> str:
    return now().isoformat(timespec="seconds")


def at(t: dtime) -> datetime:
    return datetime.combine(now().date(), t, TZ)


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        x = json.loads(path.read_text(encoding="utf-8"))
        return x if isinstance(x, dict) else None
    except Exception:
        return None


def get_json(url: str, timeout: float = 10, attempts: int = 3) -> Any:
    last: Exception | None = None
    for i in range(attempts):
        try:
            r = requests.get(url, timeout=timeout, headers={"User-Agent": UA, "Accept": "application/json", "Cache-Control": "no-cache"})
            r.raise_for_status()
            return r.json()
        except Exception as exc:
            last = exc
            if i + 1 < attempts:
                time.sleep(min(4, 0.75 * (2 ** i)))
    raise RuntimeError(f"SOURCE_GAP {url}: {type(last).__name__}: {last}")


def val(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        v = str(row.get(key, "")).strip()
        if v:
            return v
    return ""


def ordinary(code: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", code)) and not code.startswith(("0", "91"))


def normalize_companies(rows: Any, market: str) -> tuple[list[Stock], dict[str, Any]]:
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"{market}_COMPANY_SOURCE_EMPTY")
    out: dict[tuple[str, str], Stock] = {}
    rejected = Counter()
    keys: set[str] = set()
    duplicates = 0
    for row in rows:
        if not isinstance(row, dict):
            rejected["NON_OBJECT"] += 1
            continue
        keys.update(map(str, row.keys()))
        if market == "TSE":
            code = val(row, "公司代號", "SecuritiesCompanyCode", "Code").upper()
            name = val(row, "公司簡稱", "CompanyAbbreviation", "公司名稱", "CompanyName")
        else:
            code = val(row, "SecuritiesCompanyCode", "公司代號", "Code").upper()
            name = val(row, "CompanyAbbreviation", "公司簡稱", "CompanyName", "公司名稱")
        if not ordinary(code):
            rejected["NON_ORDINARY_CODE"] += 1
            continue
        if not name:
            rejected["BLANK_NAME"] += 1
            continue
        key = (market, code)
        if key in out:
            duplicates += 1
        prefix = "tse" if market == "TSE" else "otc"
        out[key] = Stock(market, code, name, f"{prefix}_{code}.tw")
    stocks = sorted(out.values(), key=lambda s: (s.market, s.code))
    return stocks, {"raw": len(rows), "accepted": len(stocks), "duplicates": duplicates, "rejected": dict(rejected), "schema_keys": sorted(keys)}


def universe() -> tuple[list[Stock], dict[str, Any]]:
    tse, a = normalize_companies(get_json(TWSE_COMPANY), "TSE")
    otc, b = normalize_companies(get_json(TPEX_COMPANY), "OTC")
    all_stocks = sorted(tse + otc, key=lambda s: (s.market, s.code))
    codes = [x.code for x in all_stocks]
    pair_keys = [(x.market, x.code) for x in all_stocks]
    gates = {
        "twse_count": len(tse) >= MIN_TWSE,
        "tpex_count": len(otc) >= MIN_TPEX,
        "total_count": MIN_TOTAL <= len(all_stocks) <= MAX_TOTAL,
        "unique_market_code": len(pair_keys) == len(set(pair_keys)),
        "global_code_unique": len(codes) == len(set(codes)),
        "twse_schema": "公司代號" in a["schema_keys"],
        "tpex_schema": "SecuritiesCompanyCode" in b["schema_keys"],
    }
    payload = json.dumps([asdict(x) for x in all_stocks], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    report = {"status": "READY" if all(gates.values()) else "SOURCE_GAP", "sources": {"TWSE": TWSE_COMPANY, "TPEX": TPEX_COMPANY}, "twse": a, "tpex": b, "total": len(all_stocks), "hash": hashlib.sha256(payload).hexdigest(), "unique_key": "market|code", "gates": gates, "checked_at": iso()}
    if report["status"] != "READY":
        raise RuntimeError("UNIVERSE_SOURCE_GAP:" + json.dumps(report, ensure_ascii=False))
    return all_stocks, report


def write_universe(stocks: list[Stock], report: dict[str, Any], day: str) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    p = OUT / f"universe_{day}.csv"
    with p.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["market", "code", "name", "ex_ch"])
        w.writeheader()
        for s in stocks:
            w.writerow(asdict(s))
    atomic_json(OUT / f"universe_{day}.json", {"report": report, "symbols": [asdict(x) for x in stocks]})
    return p


def market_gate(day: date) -> dict[str, Any]:
    if day.weekday() >= 5:
        return {"verified": True, "should_run": False, "status": "MARKET_CLOSED", "reason": "WEEKEND"}
    rows = get_json(TWSE_HOLIDAY)
    key = f"{day.year - 1911:03d}{day.month:02d}{day.day:02d}"
    matches = [x for x in rows if isinstance(x, dict) and str(x.get("Date", "")).strip() == key]
    if matches and not any("交易日" in str(x.get("Name", "")) for x in matches):
        return {"verified": True, "should_run": False, "status": "MARKET_CLOSED", "reason": "OFFICIAL_CLOSURE", "calendar": matches[0]}
    try:
        r = requests.get(DGPA, timeout=10, headers={"User-Agent": UA, "Cache-Control": "no-cache"})
        r.raise_for_status()
        r.encoding = r.apparent_encoding or r.encoding
        p = Text()
        p.feed(r.text)
        text = re.sub(r"\s+", " ", html.unescape(" ".join(p.parts)))
    except Exception as exc:
        raise RuntimeError(f"DGPA_SOURCE_GAP:{type(exc).__name__}:{exc}")
    date_match = re.search(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text)
    page_day = None
    if date_match:
        y, m, d = map(int, date_match.groups())
        try:
            page_day = date(y + 1911 if y < 1911 else y, m, d)
        except ValueError:
            page_day = None
    if page_day == day:
        pos = min([x for x in (text.find("臺北市"), text.find("台北市")) if x >= 0], default=-1)
        if pos >= 0:
            snippet = re.sub(r"\s+", "", text[max(0, pos - 60):pos + 320])
            if "停止上班" in snippet and "照常上班" not in snippet and "部分地區" not in snippet and not re.search(r"(?:下午|午後).{0,20}停止上班", snippet):
                return {"verified": True, "should_run": False, "status": "MARKET_CLOSED_EMERGENCY", "reason": "TAIPEI_WORK_SUSPENSION", "detail": snippet[:500]}
    return {"verified": True, "should_run": True, "status": "MARKET_OPEN", "reason": "NORMAL_TRADING_DAY", "dgpa_page_date": page_day.isoformat() if page_day else None}


def checkpoint(stage: str, **extra: Any) -> None:
    old = read_json(CHECKPOINT) or {}
    atomic_json(CHECKPOINT, {**old, "component": COMPONENT, "version": VERSION, "date": now().date().isoformat(), "stage": stage, "updated_at": iso(), **extra})


def preflight(mode: str) -> dict[str, Any]:
    gates = {"python": sys.version_info >= (3, 12), "engine_exists": ENGINE.exists(), "poll": POLL >= 5, "coverage": 0.8 <= PER_CYCLE <= 1, "mode": mode in {"selftest", "env_test", "live", "watchdog"}}
    return {"status": "PASS" if all(gates.values()) else "FAIL", "gates": gates, "checked_at": iso()}


def engine_env(universe_path: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.update({
        "TZ": "Asia/Taipei", "RUN_MODE": "live", "STOCKS_FILE": str(universe_path), "OUTPUT_DIR": str(OUT / "engine"),
        "POLL_SECONDS": str(POLL), "START_TIME": "08:30:00", "PREOPEN_END": "09:00:00", "END_TIME": "09:30:00",
        "BATCH_SIZE": "100", "PRIMARY_WORKERS": "4", "RETRY_BATCH_SIZE": "40", "RETRY_WORKERS": "2", "SINGLE_FALLBACK_LIMIT": "5", "SESSION_POOL_SIZE": "4",
        "PRIMARY_TIMEOUT": "1.80", "RETRY_TIMEOUT": "1.50", "SINGLE_TIMEOUT": "1.20", "CYCLE_BUDGET_SECONDS": "9.0", "CYCLE_GUARD_SECONDS": "0.25",
        "WARMUP_TIMEOUT": "3.0", "WARMUP_ATTEMPTS": "3", "SOURCE_GAP_CONSECUTIVE_CYCLES": "3", "MAX_REQUEST_ERROR_RATE": str(MAX_REQ_ERR),
        "MIN_PREOPEN_COVERAGE": "0.90", "MIN_OPEN_COVERAGE": "0.85", "MIN_SYMBOL_COVERAGE": "0.0", "MAX_ALLOWED_GAP_SECONDS": str(MAX_GAP),
        "RETRY_TRIGGER_COVERAGE": "0.90", "BACKOFF_BASE_SECONDS": "0.75", "BACKOFF_CAP_SECONDS": "4.0",
    })
    return env


def run_engine(universe_path: Path, mode: str) -> tuple[int, str]:
    env = engine_env(universe_path)
    env["RUN_MODE"] = mode
    cp = subprocess.run([sys.executable, str(ENGINE)], env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return cp.returncode, cp.stdout[-12000:]


def percentile(xs: list[float], q: float) -> float | None:
    if not xs:
        return None
    ys = sorted(xs)
    pos = (len(ys) - 1) * q
    lo = math.floor(pos)
    hi = math.ceil(pos)
    return ys[lo] if lo == hi else ys[lo] * (hi - pos) + ys[hi] * (pos - lo)


def audit_engine(stocks: list[Stock], day: str) -> dict[str, Any]:
    eng = OUT / "engine"
    raw = eng / f"raw_{day}.ndjson"
    old = read_json(eng / f"report_{day}.json") or {}
    total = len(stocks)
    coverages: list[float] = []
    pre_valid = 0
    open_valid = 0
    max_streak = 0
    streak = 0
    valid_times: list[datetime] = []
    if raw.exists():
        with raw.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                phase = str(rec.get("phase", ""))
                items = rec.get("items", []) or []
                fresh = {str(x.get("c", "")).strip() for x in items if isinstance(x, dict) and str(x.get("d", "")).strip() == day.replace("-", "") and str(x.get("c", "")).strip()}
                cov = len(fresh) / total if total else 0.0
                coverages.append(cov)
                good = cov >= PER_CYCLE
                if phase == "PREOPEN":
                    pre_valid += int(good)
                elif phase == "OPEN_VALIDATION":
                    open_valid += int(good)
                if good:
                    streak = 0
                    try:
                        valid_times.append(datetime.fromisoformat(str(rec.get("captured_at"))))
                    except Exception:
                        pass
                else:
                    streak += 1
                    max_streak = max(max_streak, streak)
    p05 = percentile(coverages, 0.05)
    gap = max(((b - a).total_seconds() for a, b in zip(valid_times, valid_times[1:])), default=None)
    req_err = float(old.get("request_error_rate", 1.0))
    expected = max(1, int(1800 / POLL))
    pre_ratio = pre_valid / expected
    open_ratio = open_valid / expected
    gates = {"raw_exists": raw.exists(), "p05_coverage": p05 is not None and p05 >= P05_MIN, "preopen_ratio": pre_ratio >= PRE_RATIO, "open_ratio": open_ratio >= OPEN_RATIO, "max_bad_streak": max_streak <= MAX_BAD_STREAK, "request_error_rate": req_err <= MAX_REQ_ERR, "max_gap": gap is not None and gap <= MAX_GAP}
    return {"pass": all(gates.values()), "gates": gates, "p05": p05, "preopen_ratio": pre_ratio, "open_ratio": open_ratio, "max_bad_streak": max_streak, "max_gap_seconds": gap, "request_error_rate": req_err, "engine_report": old}


def build_clean(stocks: list[Stock], day: str, run_id: str) -> dict[str, Any]:
    src = OUT / "engine" / f"normalized_{day}.csv"
    if not src.exists():
        return {"pass": False, "reason": "NO_ENGINE_CSV"}
    df = pd.read_csv(src, dtype={"code": str, "market_date": str})
    allowed = {s.code for s in stocks}
    compact = day.replace("-", "")
    before = len(df)
    df = df[df["code"].astype(str).isin(allowed) & (df["market_date"].astype(str) == compact)].copy()
    if {"captured_at", "code"}.issubset(df.columns):
        df = df.drop_duplicates(subset=["captured_at", "code"], keep="last")
    csv_path = OUT / f"clean_{day}_{run_id}.csv"
    pq_path = OUT / f"clean_{day}_{run_id}.parquet"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    df.to_parquet(pq_path, index=False)
    return {"pass": len(df) > 0, "rows_before": before, "rows_after": len(df), "rejected": before - len(df), "csv": str(csv_path), "parquet": str(pq_path)}


def write_status(report: dict[str, Any]) -> None:
    STATUS.mkdir(parents=True, exist_ok=True)
    day = str(report["date"])
    rid = os.getenv("GITHUB_RUN_ID", "local")
    x = {**report, "component": COMPONENT, "version": VERSION, "run_id": rid, "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", "1"), "event_name": os.getenv("GITHUB_EVENT_NAME", "local"), "source_commit": os.getenv("GITHUB_SHA", "local"), "checked_at": iso()}
    atomic_json(STATUS / "latest.json", x)
    atomic_json(STATUS / f"{day}_{rid}.json", x)
    if x.get("pass") and x.get("production_acceptance_applicable", True):
        atomic_json(STATUS / f"{day}_COMPLETED.json", x)


def selftest() -> int:
    a, ar = normalize_companies([{"公司代號": "2330", "公司簡稱": "台積電"}, {"公司代號": "0050", "公司簡稱": "ETF"}, {"公司代號": "9105", "公司簡稱": "TDR"}, {"公司代號": "2330", "公司簡稱": "台積電"}], "TSE")
    b, br = normalize_companies([{"SecuritiesCompanyCode": "6488", "CompanyAbbreviation": "環球晶"}], "OTC")
    checks = {"preflight": preflight("selftest")["status"] == "PASS", "twse": [x.code for x in a] == ["2330"], "tpex": [x.code for x in b] == ["6488"], "non_equity_filter": ar["rejected"].get("NON_ORDINARY_CODE") == 2, "duplicate": ar["duplicates"] == 1}
    rep = {"component": COMPONENT, "version": VERSION, "mode": "selftest", "pass": all(checks.values()), "checks": checks, "twse": ar, "tpex": br, "checked_at": iso()}
    atomic_json(OUT / "selftest_report.json", rep)
    print(json.dumps(rep, ensure_ascii=False))
    return 0 if rep["pass"] else 2


def env_test() -> int:
    pf = preflight("env_test")
    stocks, urep = universe()
    up = write_universe(stocks, urep, now().date().isoformat())
    rc, log = run_engine(up, "env_test")
    eng = read_json(OUT / "engine" / "env_test_report.json") or {}
    rep = {"component": COMPONENT, "version": VERSION, "mode": "env_test", "pass": pf["status"] == "PASS" and urep["status"] == "READY" and bool(eng.get("pass")), "preflight": pf, "universe": urep, "engine_returncode": rc, "engine": eng, "engine_log_tail": log[-3000:], "checked_at": iso()}
    atomic_json(OUT / "env_test_report.json", rep)
    print(json.dumps(rep, ensure_ascii=False))
    return 0 if rep["pass"] else 3


def live() -> int:
    start = now()
    day = start.date().isoformat()
    rid = os.getenv("GITHUB_RUN_ID", f"local-{start:%H%M%S}")
    if read_json(STATUS / f"{day}_COMPLETED.json"):
        return 0
    pf = preflight("live")
    if pf["status"] != "PASS":
        raise RuntimeError("PREFLIGHT_FAIL")
    checkpoint("PREFLIGHT", preflight=pf)
    stocks, urep = universe()
    up = write_universe(stocks, urep, day)
    checkpoint("SOURCE_READY", universe_hash=urep["hash"], universe_count=len(stocks))
    while now() < at(FINAL_GATE):
        time.sleep(min(1, max(0.05, (at(FINAL_GATE) - now()).total_seconds())))
    gate = market_gate(start.date())
    atomic_json(OUT / f"market_gate_{day}.json", gate)
    if not gate["should_run"]:
        rep = {"date": day, "mode": "live", "pass": True, "production_acceptance_applicable": False, "result": "MARKET_CLOSED", "preflight": pf, "universe": urep, "market_gate": gate}
        write_status(rep)
        return 0
    checkpoint("EXECUTING", market_gate=gate)
    rc, log = run_engine(up, "live")
    checkpoint("ACCEPTANCE", engine_returncode=rc)
    audit = audit_engine(stocks, day)
    clean = build_clean(stocks, day, rid)
    passed = bool(audit["pass"] and clean["pass"])
    rep = {"date": day, "mode": "live", "pass": passed, "production_acceptance_applicable": True, "preflight": pf, "universe": urep, "market_gate": gate, "engine_returncode": rc, "engine_log_tail": log[-4000:], "audit": audit, "clean": clean, "checkpoint": str(CHECKPOINT), "legacy_fixed_stocks_csv_used": False, "failure_reasons": [k.upper() for k, v in audit["gates"].items() if not v] + ([] if clean["pass"] else ["CLEAN_FAIL"])}
    checkpoint("AUDIT", pass_result=passed)
    write_status(rep)
    atomic_json(OUT / f"report_{day}.json", rep)
    print(json.dumps(rep, ensure_ascii=False))
    return 0 if passed else 4


def gh_headers() -> dict[str, str]:
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token:
        raise RuntimeError("GITHUB_TOKEN_MISSING")
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": UA}


def gh_content(repo: str, path: str) -> tuple[dict[str, Any] | None, str | None]:
    r = requests.get(f"https://api.github.com/repos/{repo}/contents/{path}", headers=gh_headers(), timeout=15)
    if r.status_code == 404:
        return None, None
    r.raise_for_status()
    p = r.json()
    obj = json.loads(base64.b64decode(p["content"]).decode())
    return (obj if isinstance(obj, dict) else None), p.get("sha")


def gh_put(repo: str, path: str, obj: dict[str, Any], msg: str) -> None:
    for i in range(3):
        _, sha = gh_content(repo, path)
        body = {"message": msg, "content": base64.b64encode(json.dumps(obj, ensure_ascii=False, indent=2).encode()).decode(), "branch": os.getenv("DEFAULT_BRANCH", "main")}
        if sha:
            body["sha"] = sha
        r = requests.put(f"https://api.github.com/repos/{repo}/contents/{path}", headers=gh_headers(), json=body, timeout=15)
        if r.status_code in (200, 201):
            return
        if r.status_code in (409, 422) and i < 2:
            time.sleep(i + 1)
            continue
        raise RuntimeError(f"GITHUB_WRITE_{r.status_code}:{r.text[:400]}")


def parse_dt(x: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(x).replace("Z", "+00:00")).astimezone(TZ) if x else None
    except Exception:
        return None


def run_state(repo: str, workflow: str, day: date) -> dict[str, Any]:
    r = requests.get(f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs?per_page=100", headers=gh_headers(), timeout=15)
    r.raise_for_status()
    obs = []
    current = str(os.getenv("GITHUB_RUN_ID", ""))
    for run in r.json().get("workflow_runs", []):
        rid = str(run.get("id", ""))
        created = parse_dt(run.get("created_at"))
        if not rid or rid == current or not created or created.date() != day:
            continue
        j = requests.get(f"https://api.github.com/repos/{repo}/actions/runs/{rid}/jobs?per_page=100", headers=gh_headers(), timeout=15)
        j.raise_for_status()
        for job in j.json().get("jobs", []):
            if str(job.get("name")) != "production":
                continue
            obs.append({"run_id": int(rid), "job_id": job.get("id"), "status": job.get("status"), "conclusion": job.get("conclusion"), "started_at": job.get("started_at"), "event": run.get("event")})
    return {"observations": obs, "healthy": [x for x in obs if x["status"] == "in_progress" and x["started_at"]], "queued": [x for x in obs if x["status"] in {"queued", "waiting", "pending", "requested"} and not x["started_at"]], "failures": [x for x in obs if x["status"] == "completed" and x["conclusion"] not in {None, "success"}], "successes": [x for x in obs if x["status"] == "completed" and x["conclusion"] == "success"]}


def dispatch(repo: str, workflow: str) -> None:
    body = {"ref": os.getenv("DEFAULT_BRANCH", "main"), "inputs": {"run_mode": "live_recovery"}}
    r = requests.post(f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches", headers=gh_headers(), json=body, timeout=15)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"DISPATCH_{r.status_code}:{r.text[:400]}")


def watchdog(stage: str) -> int:
    repo = os.getenv("GITHUB_REPOSITORY", "").strip()
    workflow = os.getenv("A4_WORKFLOW_FILE", "scraper.yml")
    day = now().date()
    ds = day.isoformat()
    if not repo:
        raise RuntimeError("GITHUB_REPOSITORY_MISSING")
    completed, _ = gh_content(repo, f"status/a4/{ds}_COMPLETED.json")
    if completed and completed.get("pass"):
        status = "HEALTHY_NOOP"; reason = "COMPLETED_EXISTS"; state = {}; recover = False
    else:
        state = run_state(repo, workflow, day)
        if state["healthy"]:
            status = "HEALTHY_NOOP"; reason = "PRODUCTION_IN_PROGRESS"; recover = False
        elif state["queued"]:
            status = "A4_RUNNER_QUEUE_RISK"; reason = "PRODUCTION_QUEUED_DO_NOT_CANCEL"; recover = False
        elif state["successes"]:
            status = "HEALTHY_NOOP"; reason = "SUCCESS_AWAITING_COMPLETED"; recover = False
        else:
            status = "RECOVERY_REQUIRED"; reason = "NO_HEALTHY_PRODUCTION" if not state["observations"] else "PRODUCTION_FAILED"; recover = True
    desired = 1 if stage == "first" else 2
    dispatched = False
    trigger = None
    if recover:
        old, _ = gh_content(repo, "status/a4/recovery_trigger.json")
        old_attempt = int(old.get("attempt", 0)) if old and old.get("date") == ds else 0
        if old_attempt < desired:
            trigger = {"component": COMPONENT, "status": "RECOVERY_REQUESTED", "date": ds, "attempt": desired, "requested_at": iso(), "reason": "no_healthy_runner_0815" if stage == "first" else "no_healthy_runner_0829", "requested_by": "QTS_A4_COMPLETE_CONTROL_POINT"}
            gh_put(repo, "status/a4/recovery_trigger.json", trigger, f"A4 recovery {ds} attempt {desired}")
            dispatch(repo, workflow)
            dispatched = True
        else:
            status = "RECOVERY_ALREADY_REQUESTED"; reason = f"ATTEMPT_ALREADY_{old_attempt}"
    decision = {"component": COMPONENT, "version": VERSION, "mode": "watchdog", "stage": stage, "date": ds, "status": status, "reason": reason, "desired_attempt": desired, "recovery_dispatched": dispatched, "trigger": trigger, "state": state, "checked_at": iso()}
    gh_put(repo, "status/a4/watchdog_latest.json", decision, f"A4 watchdog {ds} {stage} {status}")
    atomic_json(OUT / "watchdog_latest.json", decision)
    print(json.dumps(decision, ensure_ascii=False))
    return 0


def fatal(mode: str, exc: Exception) -> None:
    rep = {"component": COMPONENT, "version": VERSION, "date": now().date().isoformat(), "mode": mode, "pass": False, "failure_reasons": ["FAIL_CLOSED"], "error": f"{type(exc).__name__}: {exc}", "checked_at": iso()}
    atomic_json(OUT / "fatal_error.json", rep)
    if mode == "live":
        try:
            write_status(rep)
        except Exception:
            pass


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--mode", required=True, choices=["selftest", "env_test", "live", "watchdog"])
    p.add_argument("--watchdog-stage", choices=["first", "final"], default="first")
    a = p.parse_args()
    return selftest() if a.mode == "selftest" else env_test() if a.mode == "env_test" else watchdog(a.watchdog_stage) if a.mode == "watchdog" else live()


if __name__ == "__main__":
    mode = "unknown"
    try:
        if "--mode" in sys.argv:
            mode = sys.argv[sys.argv.index("--mode") + 1]
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        fatal(mode, exc)
        print(json.dumps({"component": COMPONENT, "version": VERSION, "mode": mode, "result": "FAIL_CLOSED", "error": f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        raise SystemExit(99)
