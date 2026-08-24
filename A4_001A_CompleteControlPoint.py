from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import html
import json
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

VERSION = "A4_001A_COMPLETE_CONTROL_POINT_V3.0.0"
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
UA = "Mozilla/5.0 QTS-A4-Complete-Control-Point/3.0"
START = dtime(8, 30)
END = dtime(9, 30)
FINAL_GATE = dtime(8, 27, 0)
MIN_TWSE = int(os.getenv("MIN_TWSE_SYMBOLS", "800"))
MIN_TPEX = int(os.getenv("MIN_TPEX_SYMBOLS", "600"))
MIN_TOTAL = int(os.getenv("MIN_TOTAL_SYMBOLS", "1500"))
MAX_TOTAL = int(os.getenv("MAX_TOTAL_SYMBOLS", "3000"))
POLL = float(os.getenv("POLL_SECONDS", "30"))
MIN_COVERAGE = float(os.getenv("MIN_SYMBOL_COVERAGE", "0.995"))
MAX_REQ_ERR = float(os.getenv("MAX_REQUEST_ERROR_RATE", "0.02"))
MIN_PRE_RATIO = float(os.getenv("MIN_PREOPEN_VALID_CYCLES", "0.95"))
MIN_OPEN_RATIO = float(os.getenv("MIN_OPEN_VALID_CYCLES", "0.90"))
MAX_BAD_STREAK = int(os.getenv("MAX_CONSECUTIVE_INVALID_CYCLES", "2"))

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
        if data.strip(): self.parts.append(data.strip())

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
            if i + 1 < attempts: time.sleep(min(4.0, 0.75 * (2 ** i)))
    raise RuntimeError(f"SOURCE_GAP {url}: {type(last).__name__}: {last}")

def val(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        v = str(row.get(key, "")).strip()
        if v: return v
    return ""

def ordinary(code: str) -> bool:
    return bool(re.fullmatch(r"\d{4}", code)) and not code.startswith(("0", "91"))

def normalize_companies(rows: Any, market: str) -> tuple[list[Stock], dict[str, Any]]:
    if not isinstance(rows, list) or not rows: raise RuntimeError(f"{market}_COMPANY_SOURCE_EMPTY")
    out: dict[tuple[str, str], Stock] = {}
    rejected = Counter(); keys: set[str] = set(); duplicates = 0
    for row in rows:
        if not isinstance(row, dict): rejected["NON_OBJECT"] += 1; continue
        keys.update(map(str, row.keys()))
        if market == "TSE":
            code = val(row, "公司代號", "SecuritiesCompanyCode", "Code").upper()
            name = val(row, "公司簡稱", "CompanyAbbreviation", "公司名稱", "CompanyName")
        else:
            code = val(row, "SecuritiesCompanyCode", "公司代號", "Code").upper()
            name = val(row, "CompanyAbbreviation", "公司簡稱", "CompanyName", "公司名稱")
        if not ordinary(code): rejected["NON_ORDINARY_CODE"] += 1; continue
        if not name: rejected["BLANK_NAME"] += 1; continue
        key = (market, code)
        if key in out: duplicates += 1
        out[key] = Stock(market, code, name, f"{'tse' if market == 'TSE' else 'otc'}_{code}.tw")
    stocks = sorted(out.values(), key=lambda s: (s.market, s.code))
    return stocks, {"raw": len(rows), "accepted": len(stocks), "duplicates": duplicates, "rejected": dict(rejected), "schema_keys": sorted(keys)}

def universe() -> tuple[list[Stock], dict[str, Any]]:
    tse, a = normalize_companies(get_json(TWSE_COMPANY), "TSE")
    otc, b = normalize_companies(get_json(TPEX_COMPANY), "OTC")
    stocks = sorted(tse + otc, key=lambda s: (s.market, s.code))
    pairs = [(x.market, x.code) for x in stocks]; codes = [x.code for x in stocks]
    gates = {"twse_count": len(tse) >= MIN_TWSE, "tpex_count": len(otc) >= MIN_TPEX, "total_count": MIN_TOTAL <= len(stocks) <= MAX_TOTAL, "unique_market_code": len(pairs) == len(set(pairs)), "global_code_unique": len(codes) == len(set(codes)), "twse_schema": "公司代號" in a["schema_keys"], "tpex_schema": "SecuritiesCompanyCode" in b["schema_keys"]}
    payload = json.dumps([asdict(x) for x in stocks], ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    rep = {"status": "READY" if all(gates.values()) else "SOURCE_GAP", "sources": {"TWSE": TWSE_COMPANY, "TPEX": TPEX_COMPANY}, "twse": a, "tpex": b, "total": len(stocks), "hash": hashlib.sha256(payload).hexdigest(), "unique_key": "market|code", "gates": gates, "checked_at": iso()}
    if rep["status"] != "READY": raise RuntimeError("UNIVERSE_SOURCE_GAP:" + json.dumps(rep, ensure_ascii=False))
    return stocks, rep

def write_universe(stocks: list[Stock], report: dict[str, Any], day: str) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    p = OUT / f"universe_{day}.csv"
    with p.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["market", "code", "name", "ex_ch"]); w.writeheader()
        for s in stocks: w.writerow(asdict(s))
    atomic_json(OUT / f"universe_{day}.json", {"report": report, "symbols": [asdict(x) for x in stocks]})
    return p

def market_gate(day: date) -> dict[str, Any]:
    if day.weekday() >= 5: return {"verified": True, "should_run": False, "status": "MARKET_CLOSED", "reason": "WEEKEND"}
    rows = get_json(TWSE_HOLIDAY); key = f"{day.year - 1911:03d}{day.month:02d}{day.day:02d}"
    matches = [x for x in rows if isinstance(x, dict) and str(x.get("Date", "")).strip() == key]
    if matches and not any("交易日" in str(x.get("Name", "")) for x in matches): return {"verified": True, "should_run": False, "status": "MARKET_CLOSED", "reason": "OFFICIAL_CLOSURE", "calendar": matches[0]}
    try:
        r = requests.get(DGPA, timeout=10, headers={"User-Agent": UA, "Cache-Control": "no-cache"}); r.raise_for_status(); r.encoding = r.apparent_encoding or r.encoding
        p = Text(); p.feed(r.text); text = re.sub(r"\s+", " ", html.unescape(" ".join(p.parts)))
    except Exception as exc: raise RuntimeError(f"DGPA_SOURCE_GAP:{type(exc).__name__}:{exc}")
    date_match = re.search(r"(\d{2,3})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日", text); page_day = None
    if date_match:
        y, m, d = map(int, date_match.groups())
        try: page_day = date(y + 1911 if y < 1911 else y, m, d)
        except ValueError: page_day = None
    if page_day == day:
        pos = min([x for x in (text.find("臺北市"), text.find("台北市")) if x >= 0], default=-1)
        if pos >= 0:
            snippet = re.sub(r"\s+", "", text[max(0, pos - 60):pos + 320])
            if "停止上班" in snippet and "照常上班" not in snippet and "部分地區" not in snippet: return {"verified": True, "should_run": False, "status": "MARKET_CLOSED_EMERGENCY", "reason": "TAIPEI_WORK_SUSPENSION", "detail": snippet[:500]}
    return {"verified": True, "should_run": True, "status": "MARKET_OPEN", "reason": "NORMAL_TRADING_DAY", "dgpa_page_date": page_day.isoformat() if page_day else None}

def checkpoint(stage: str, **extra: Any) -> None:
    old = read_json(CHECKPOINT) or {}
    atomic_json(CHECKPOINT, {**old, "component": COMPONENT, "version": VERSION, "date": now().date().isoformat(), "stage": stage, "updated_at": iso(), **extra})

def preflight(mode: str) -> dict[str, Any]:
    gates = {"python": sys.version_info >= (3, 12), "engine_exists": ENGINE.exists(), "poll": POLL == 30, "coverage": 0.95 <= MIN_COVERAGE <= 1.0, "mode": mode in {"selftest", "env_test", "qualify", "live", "watchdog"}}
    return {"status": "PASS" if all(gates.values()) else "FAIL", "gates": gates, "checked_at": iso()}

def engine_env(universe_path: Path, engine_out: Path, mode: str) -> dict[str, str]:
    env = os.environ.copy()
    env.update({"TZ": "Asia/Taipei", "RUN_MODE": mode, "STOCKS_FILE": str(universe_path), "OUTPUT_DIR": str(engine_out), "POLL_SECONDS": "30", "START_TIME": "08:30:00", "PREOPEN_END": "09:00:00", "END_TIME": "09:30:00", "TARGET_BATCH_SYMBOLS": "90", "MIN_BATCH_SYMBOLS": "8", "MAX_URL_CHARS": "1800", "MIN_REQUEST_INTERVAL": "0.25", "REQUEST_TIMEOUT": "3.0", "REQUEST_RETRIES": "2", "CYCLE_BUDGET_SECONDS": "20.0", "CYCLE_GUARD_SECONDS": "0.50", "BACKOFF_BASE_SECONDS": "0.60", "BACKOFF_CAP_SECONDS": "4.0", "MIN_SYMBOL_COVERAGE": str(MIN_COVERAGE), "MAX_TERMINAL_ERROR_RATE": str(MAX_REQ_ERR), "SOURCE_GAP_CONSECUTIVE_CYCLES": "3", "NO_QUOTE_CONFIRMATIONS": "3", "NO_QUOTE_PROBE_LIMIT": "30"})
    return env

def run_engine(universe_path: Path, mode: str, engine_out: Path) -> tuple[int, str]:
    engine_out.mkdir(parents=True, exist_ok=True)
    p = subprocess.run([sys.executable, str(ENGINE)], env=engine_env(universe_path, engine_out, mode), text=True, capture_output=True, timeout=180 if mode == "env_test" else None)
    return int(p.returncode), ((p.stdout or "") + "\n" + (p.stderr or ""))[-12000:]

def env_test_once(stocks: list[Stock], universe_path: Path, out_dir: Path) -> dict[str, Any]:
    rc, log = run_engine(universe_path, "env_test", out_dir); eng = read_json(out_dir / "env_test_report.json") or {}
    requested = int(eng.get("requested_symbols") or 0); returned = int(eng.get("returned_symbols") or 0); coverage = float(eng.get("coverage") or 0); raw_coverage = float(eng.get("raw_coverage") or 0); attempts = int(eng.get("request_attempts") or 0); errors = int(eng.get("request_errors") or 0); error_rate = float(eng.get("request_error_rate") if eng.get("request_error_rate") is not None else 1.0); elapsed = float(eng.get("cycle_elapsed_seconds") or 999999); budget = float(eng.get("cycle_budget_seconds") or 0); warmup_ok = int(eng.get("warmup_ok_count") or 0)
    gates = {"engine_clean_exit": rc == 0, "universe_matches": requested == len(stocks), "coverage": coverage >= MIN_COVERAGE, "request_error_rate": error_rate <= MAX_REQ_ERR, "cycle_budget": budget > 0 and elapsed <= budget + 0.5, "warmup": warmup_ok > 0}
    return {"pass": all(gates.values()), "gates": gates, "requested_symbols": requested, "returned_symbols": returned, "coverage": coverage, "raw_coverage": raw_coverage, "request_attempts": attempts, "request_errors": errors, "request_error_rate": error_rate, "cycle_elapsed_seconds": elapsed, "cycle_budget_seconds": budget, "safe_batch_symbols": eng.get("safe_batch_symbols"), "adaptive_splits": eng.get("adaptive_splits"), "missing_symbols": eng.get("missing_symbols", []), "no_quote_symbols": eng.get("no_quote_symbols", []), "sample_errors": eng.get("sample_errors", []), "engine_returncode": rc, "engine_log_tail": log[-2500:]}

def qualification(stocks: list[Stock], universe_path: Path, report_path: Path) -> dict[str, Any]:
    rounds: list[dict[str, Any]] = []
    for rnd in (1, 2, 3):
        one = {"round": rnd, **env_test_once(stocks, universe_path, OUT / "qualification" / f"round_{rnd}")}; rounds.append(one)
        rep = {"component": COMPONENT, "version": VERSION, "status": "ROUND_PASS" if one["pass"] else "ROUND_FAIL", "rounds_required": 3, "rounds_passed": sum(1 for x in rounds if x["pass"]), "current_round": rnd, "rounds": rounds, "checked_at": iso()}; atomic_json(report_path, rep)
        if not one["pass"]: return {**rep, "pass": False}
        if rnd < 3: time.sleep(5)
    final = {"component": COMPONENT, "version": VERSION, "status": "PASS_3X", "pass": True, "rounds_required": 3, "rounds_passed": 3, "rounds": rounds, "checked_at": iso()}; atomic_json(report_path, final); return final

def audit_engine(day: str) -> dict[str, Any]:
    eng = OUT / "engine"; raw = eng / f"raw_{day}.ndjson"; csv_path = eng / f"normalized_{day}.csv"; report = read_json(eng / f"report_{day}.json") or {}
    p05 = report.get("p05_coverage"); pre_ratio = float(report.get("preopen_valid_ratio") or 0); open_ratio = float(report.get("open_valid_ratio") or 0); req_err = float(report.get("request_error_rate") if report.get("request_error_rate") is not None else 1.0); max_bad = int(report.get("max_consecutive_failed_cycles") or 999)
    gates = {"raw_exists": raw.exists() and raw.stat().st_size > 0, "normalized_exists": csv_path.exists() and csv_path.stat().st_size > 0, "engine_report_pass": bool(report.get("pass")), "p05_coverage": p05 is not None and float(p05) >= MIN_COVERAGE, "preopen_ratio": pre_ratio >= MIN_PRE_RATIO, "open_ratio": open_ratio >= MIN_OPEN_RATIO, "max_bad_streak": max_bad <= MAX_BAD_STREAK, "request_error_rate": req_err <= MAX_REQ_ERR, "capture_window_present": bool(report.get("first_capture") and report.get("last_capture"))}
    return {"pass": all(gates.values()), "gates": gates, "p05": p05, "preopen_ratio": pre_ratio, "open_ratio": open_ratio, "max_bad_streak": max_bad, "request_error_rate": req_err, "engine_report": report}

def build_clean(stocks: list[Stock], day: str, run_id: str) -> dict[str, Any]:
    src = OUT / "engine" / f"normalized_{day}.csv"
    if not src.exists(): return {"pass": False, "reason": "NO_ENGINE_CSV"}
    df = pd.read_csv(src, dtype={"code": str, "market_date": str}); allowed = {s.code for s in stocks}; compact = day.replace("-", ""); before = len(df)
    df = df[df["code"].astype(str).isin(allowed) & (df["market_date"].astype(str) == compact)].copy()
    if {"captured_at", "code"}.issubset(df.columns): df = df.drop_duplicates(subset=["captured_at", "code"], keep="last")
    csv_path = OUT / f"clean_{day}_{run_id}.csv"; pq_path = OUT / f"clean_{day}_{run_id}.parquet"; df.to_csv(csv_path, index=False, encoding="utf-8-sig"); df.to_parquet(pq_path, index=False)
    return {"pass": len(df) > 0, "rows_before": before, "rows_after": len(df), "rejected": before-len(df), "csv": str(csv_path), "parquet": str(pq_path)}

def write_status(report: dict[str, Any]) -> None:
    STATUS.mkdir(parents=True, exist_ok=True); day = str(report["date"]); rid = os.getenv("GITHUB_RUN_ID", "local")
    x = {**report, "component": COMPONENT, "version": VERSION, "run_id": rid, "run_attempt": os.getenv("GITHUB_RUN_ATTEMPT", "1"), "event_name": os.getenv("GITHUB_EVENT_NAME", "local"), "source_commit": os.getenv("GITHUB_SHA", "local"), "checked_at": iso()}
    atomic_json(STATUS / "latest.json", x); atomic_json(STATUS / f"{day}_{rid}.json", x)
    if x.get("pass") and x.get("production_acceptance_applicable", True): atomic_json(STATUS / f"{day}_COMPLETED.json", x)

def selftest() -> int:
    a, ar = normalize_companies([{"公司代號":"2330","公司簡稱":"台積電"},{"公司代號":"0050","公司簡稱":"ETF"},{"公司代號":"9105","公司簡稱":"TDR"},{"公司代號":"2330","公司簡稱":"台積電"}], "TSE"); b, br = normalize_companies([{"SecuritiesCompanyCode":"6488","CompanyAbbreviation":"環球晶"}], "OTC")
    checks = {"preflight": preflight("selftest")["status"] == "PASS", "twse": [x.code for x in a] == ["2330"], "tpex": [x.code for x in b] == ["6488"], "non_equity_filter": ar["rejected"].get("NON_ORDINARY_CODE") == 2, "duplicate": ar["duplicates"] == 1}
    rep = {"component": COMPONENT, "version": VERSION, "mode": "selftest", "pass": all(checks.values()), "checks": checks, "twse": ar, "tpex": br, "checked_at": iso()}; atomic_json(OUT / "selftest_report.json", rep); print(json.dumps(rep, ensure_ascii=False)); return 0 if rep["pass"] else 2

def env_test() -> int:
    pf = preflight("env_test"); stocks, urep = universe(); up = write_universe(stocks, urep, now().date().isoformat()); gate = market_gate(now().date()); result = env_test_once(stocks, up, OUT / "engine_env_test")
    rep = {"component": COMPONENT, "version": VERSION, "mode": "env_test", "pass": pf["status"] == "PASS" and urep["status"] == "READY" and bool(gate.get("verified")) and result["pass"], "preflight": pf, "universe": urep, "market_gate_probe": gate, **result, "checked_at": iso()}; atomic_json(OUT / "env_test_report.json", rep); print(json.dumps(rep, ensure_ascii=False)); return 0 if rep["pass"] else 3

def qualify() -> int:
    pf = preflight("qualify"); stocks, urep = universe(); day = now().date().isoformat(); up = write_universe(stocks, urep, day); rep = qualification(stocks, up, STATUS / "qualification_latest.json"); full = {"component": COMPONENT, "version": VERSION, "mode": "qualify", "preflight": pf, "universe": urep, **rep}; atomic_json(OUT / "qualification_report.json", full); print(json.dumps(full, ensure_ascii=False)); return 0 if pf["status"] == "PASS" and rep.get("pass") else 5

def live() -> int:
    started = now(); day = started.date().isoformat(); rid = os.getenv("GITHUB_RUN_ID", f"local-{started:%H%M%S}")
    if read_json(STATUS / f"{day}_COMPLETED.json"): return 0
    pf = preflight("live")
    if pf["status"] != "PASS": raise RuntimeError("PREFLIGHT_FAIL")
    checkpoint("PREFLIGHT", preflight=pf); stocks, urep = universe(); up = write_universe(stocks, urep, day); checkpoint("SOURCE_READY", universe_hash=urep["hash"], universe_count=len(stocks))
    gate = market_gate(started.date()); atomic_json(OUT / f"market_gate_{day}.json", gate)
    if not gate["should_run"]:
        rep = {"date": day, "mode": "live", "pass": True, "production_acceptance_applicable": False, "result": "MARKET_CLOSED", "preflight": pf, "universe": urep, "market_gate": gate}; write_status(rep); return 0
    qual = qualification(stocks, up, STATUS / "qualification_latest.json"); checkpoint("QUALIFICATION", qualification_pass=bool(qual.get("pass")))
    while now() < at(FINAL_GATE): time.sleep(min(1.0, max(0.05, (at(FINAL_GATE) - now()).total_seconds())))
    checkpoint("EXECUTING", market_gate=gate, qualification_pass=bool(qual.get("pass"))); rc, log = run_engine(up, "live", OUT / "engine"); checkpoint("ACCEPTANCE", engine_returncode=rc)
    audit = audit_engine(day); clean = build_clean(stocks, day, rid); engine_clean_exit = rc == 0; qualification_pass = bool(qual.get("pass")); passed = bool(engine_clean_exit and audit["pass"] and clean["pass"] and qualification_pass)
    reasons = [k.upper() for k,v in audit["gates"].items() if not v] + ([] if clean["pass"] else ["CLEAN_FAIL"]) + ([] if engine_clean_exit else ["ENGINE_NONZERO_EXIT"]) + ([] if qualification_pass else ["QUALIFICATION_3X_FAIL"])
    rep = {"date": day, "mode": "live", "pass": passed, "production_acceptance_applicable": True, "preflight": pf, "universe": urep, "market_gate": gate, "qualification": qual, "engine_returncode": rc, "engine_clean_exit": engine_clean_exit, "engine_log_tail": log[-5000:], "audit": audit, "clean": clean, "checkpoint": str(CHECKPOINT), "legacy_fixed_stocks_csv_used": False, "failure_reasons": reasons}; checkpoint("AUDIT", pass_result=passed); write_status(rep); atomic_json(OUT / f"report_{day}.json", rep); print(json.dumps(rep, ensure_ascii=False)); return 0 if passed else 4

def gh_headers() -> dict[str,str]:
    token = os.getenv("GITHUB_TOKEN", "").strip()
    if not token: raise RuntimeError("GITHUB_TOKEN_MISSING")
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": UA}

def gh_content(repo:str,path:str)->tuple[dict[str,Any]|None,str|None]:
    r=requests.get(f"https://api.github.com/repos/{repo}/contents/{path}",headers=gh_headers(),timeout=15)
    if r.status_code==404:return None,None
    r.raise_for_status(); p=r.json(); obj=json.loads(base64.b64decode(p["content"]).decode()); return (obj if isinstance(obj,dict) else None),p.get("sha")

def gh_put(repo:str,path:str,obj:dict[str,Any],msg:str)->None:
    for i in range(3):
        _,sha=gh_content(repo,path); body={"message":msg,"content":base64.b64encode(json.dumps(obj,ensure_ascii=False,indent=2).encode()).decode(),"branch":os.getenv("DEFAULT_BRANCH","main")}
        if sha: body["sha"]=sha
        r=requests.put(f"https://api.github.com/repos/{repo}/contents/{path}",headers=gh_headers(),json=body,timeout=15)
        if r.status_code in (200,201): return
        if r.status_code in (409,422) and i<2: time.sleep(i+1); continue
        raise RuntimeError(f"GITHUB_WRITE_{r.status_code}:{r.text[:400]}")

def parse_dt(x:Any)->datetime|None:
    try:return datetime.fromisoformat(str(x).replace("Z","+00:00")).astimezone(TZ) if x else None
    except Exception:return None

def run_state(repo:str,workflow:str,day:date)->dict[str,Any]:
    r=requests.get(f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/runs?per_page=100",headers=gh_headers(),timeout=15); r.raise_for_status(); obs=[]; current=str(os.getenv("GITHUB_RUN_ID",""))
    for run in r.json().get("workflow_runs",[]):
        rid=str(run.get("id","")); created=parse_dt(run.get("created_at"))
        if not rid or rid==current or not created or created.date()!=day: continue
        j=requests.get(f"https://api.github.com/repos/{repo}/actions/runs/{rid}/jobs?per_page=100",headers=gh_headers(),timeout=15); j.raise_for_status()
        for job in j.json().get("jobs",[]):
            if str(job.get("name"))!="control": continue
            obs.append({"run_id":int(rid),"job_id":job.get("id"),"status":job.get("status"),"conclusion":job.get("conclusion"),"started_at":job.get("started_at"),"event":run.get("event")})
    return {"observations":obs,"healthy":[x for x in obs if x["status"]=="in_progress" and x["started_at"]],"queued":[x for x in obs if x["status"] in {"queued","waiting","pending","requested"} and not x["started_at"]],"failures":[x for x in obs if x["status"]=="completed" and x["conclusion"] not in {None,"success"}],"successes":[x for x in obs if x["status"]=="completed" and x["conclusion"]=="success"]}

def dispatch(repo:str,workflow:str)->None:
    r=requests.post(f"https://api.github.com/repos/{repo}/actions/workflows/{workflow}/dispatches",headers=gh_headers(),json={"ref":os.getenv("DEFAULT_BRANCH","main"),"inputs":{"run_mode":"live_recovery"}},timeout=15)
    if r.status_code not in (200,204): raise RuntimeError(f"DISPATCH_{r.status_code}:{r.text[:400]}")

def watchdog(stage:str)->int:
    repo=os.getenv("GITHUB_REPOSITORY","").strip(); workflow=os.getenv("A4_WORKFLOW_FILE","scraper.yml"); day=now().date(); ds=day.isoformat()
    if not repo: raise RuntimeError("GITHUB_REPOSITORY_MISSING")
    completed,_=gh_content(repo,f"status/a4/{ds}_COMPLETED.json")
    if completed and completed.get("pass"): status="HEALTHY_NOOP"; reason="COMPLETED_EXISTS"; state={}; recover=False
    else:
        state=run_state(repo,workflow,day)
        if state["healthy"]: status="HEALTHY_NOOP"; reason="CONTROL_IN_PROGRESS"; recover=False
        elif state["queued"]: status="A4_RUNNER_QUEUE_RISK"; reason="CONTROL_QUEUED_DO_NOT_CANCEL"; recover=False
        elif state["successes"]: status="HEALTHY_NOOP"; reason="SUCCESS_AWAITING_COMPLETED"; recover=False
        else: status="RECOVERY_REQUIRED"; reason="NO_HEALTHY_CONTROL" if not state["observations"] else "CONTROL_FAILED"; recover=True
    desired=1 if stage=="first" else 2; dispatched=False; trigger=None
    if recover:
        old,_=gh_content(repo,"status/a4/recovery_trigger.json"); old_attempt=int(old.get("attempt",0)) if old and old.get("date")==ds else 0
        if old_attempt<desired:
            trigger={"component":COMPONENT,"status":"RECOVERY_REQUESTED","date":ds,"attempt":desired,"requested_at":iso(),"reason":"no_healthy_runner_0817" if stage=="first" else "no_healthy_runner_0822","requested_by":"QTS_A4_COMPLETE_CONTROL_POINT_V3"}; gh_put(repo,"status/a4/recovery_trigger.json",trigger,f"A4 recovery {ds} attempt {desired}"); dispatch(repo,workflow); dispatched=True
        else: status="RECOVERY_ALREADY_REQUESTED"; reason=f"ATTEMPT_ALREADY_{old_attempt}"
    decision={"component":COMPONENT,"version":VERSION,"mode":"watchdog","stage":stage,"date":ds,"status":status,"reason":reason,"desired_attempt":desired,"recovery_dispatched":dispatched,"trigger":trigger,"state":state,"checked_at":iso()}; gh_put(repo,"status/a4/watchdog_latest.json",decision,f"A4 watchdog {ds} {stage} {status}"); atomic_json(OUT/"watchdog_latest.json",decision); print(json.dumps(decision,ensure_ascii=False)); return 0

def fatal(mode:str,exc:Exception)->None:
    rep={"component":COMPONENT,"version":VERSION,"date":now().date().isoformat(),"mode":mode,"pass":False,"failure_reasons":["FAIL_CLOSED"],"error":f"{type(exc).__name__}: {exc}","checked_at":iso()}; atomic_json(OUT/"fatal_error.json",rep)
    if mode=="live":
        try: write_status(rep)
        except Exception: pass

def main()->int:
    p=argparse.ArgumentParser(); p.add_argument("--mode",required=True,choices=["selftest","env_test","qualify","live","watchdog"]); p.add_argument("--watchdog-stage",choices=["first","final"],default="first"); a=p.parse_args()
    return selftest() if a.mode=="selftest" else env_test() if a.mode=="env_test" else qualify() if a.mode=="qualify" else watchdog(a.watchdog_stage) if a.mode=="watchdog" else live()

if __name__=="__main__":
    mode="unknown"
    try:
        if "--mode" in sys.argv: mode=sys.argv[sys.argv.index("--mode")+1]
        raise SystemExit(main())
    except KeyboardInterrupt: raise
    except Exception as exc:
        fatal(mode,exc); print(json.dumps({"component":COMPONENT,"version":VERSION,"mode":mode,"result":"FAIL_CLOSED","error":f"{type(exc).__name__}: {exc}"},ensure_ascii=False),file=sys.stderr); raise SystemExit(99)
