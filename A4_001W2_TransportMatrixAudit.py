from __future__ import annotations

import importlib.util
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
ROOT = Path(__file__).resolve().parent
ADAPTER = ROOT / "A4_001P_PlaceholderAwareEntrypoint.py"
VERSION = "A4_001W2_TRANSPORT_MATRIX_V1.0.0"
OUT = Path(os.getenv("A4_MATRIX_OUT", "output/transport_matrix"))
CANDIDATES = [120, 100, 80, 60, 50]
CYCLES_PER_CANDIDATE = int(os.getenv("A4_MATRIX_CYCLES", "10"))
INTERVAL_SECONDS = float(os.getenv("A4_MATRIX_INTERVAL", "5"))


def load_adapter():
    spec = importlib.util.spec_from_file_location("a4_placeholder_adapter_matrix", ADAPTER)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load adapter")
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def run_candidate(core: Any, stocks: list[Any], batch_size: int) -> dict[str, Any]:
    core.BATCH_SIZE = batch_size
    core.PRIMARY_WORKERS = 1
    core.RETRY_BATCH_SIZE = min(25, batch_size)
    core.RETRY_WORKERS = 1
    core.SINGLE_FALLBACK_LIMIT = 8
    core.PRIMARY_TIMEOUT = 1.75
    core.RETRY_TIMEOUT = 1.0
    core.SINGLE_TIMEOUT = 0.65
    core.CYCLE_BUDGET_SECONDS = 4.60

    rows=[]
    next_target=time.monotonic()
    for idx in range(CYCLES_PER_CANDIDATE):
        now=time.monotonic()
        if now < next_target:
            time.sleep(next_target-now)
        started=time.monotonic()
        results,diag=core.capture_cycle(stocks, started+core.CYCLE_BUDGET_SECONDS)
        elapsed=time.monotonic()-started
        merged=results[0]
        returned=len({str(x.get('c','')) for x in merged.items if isinstance(x,dict) and str(x.get('c',''))})
        row={
            'cycle':idx,
            'batch_size':batch_size,
            'requested_symbols':len(stocks),
            'returned_symbols':returned,
            'missing_symbols':diag.get('missing_symbols',[]),
            'placeholder_symbols':diag.get('placeholder_symbols',[]),
            'request_attempts':diag.get('request_attempts',0),
            'request_errors':diag.get('request_errors',0),
            'elapsed_seconds':elapsed,
            'pass':returned==len(stocks) and not diag.get('missing_symbols') and elapsed <= core.CYCLE_BUDGET_SECONDS+0.25,
        }
        rows.append(row)
        print(json.dumps(row,ensure_ascii=False),flush=True)
        next_target += INTERVAL_SECONDS
        if next_target < time.monotonic():
            next_target=time.monotonic()

    ok=sum(1 for r in rows if r['pass'])
    return {
        'batch_size':batch_size,
        'primary_workers':1,
        'cycles':len(rows),
        'successful_cycles':ok,
        'success_rate':ok/len(rows) if rows else 0.0,
        'max_elapsed_seconds':max((r['elapsed_seconds'] for r in rows),default=0.0),
        'avg_elapsed_seconds':mean([r['elapsed_seconds'] for r in rows]) if rows else 0.0,
        'total_request_errors':sum(int(r.get('request_errors') or 0) for r in rows),
        'failed_cycles':[r for r in rows if not r['pass']],
        'placeholder_union':sorted({c for r in rows for c in (r.get('placeholder_symbols') or [])}),
        'pass':ok==len(rows),
    }


def main() -> int:
    OUT.mkdir(parents=True,exist_ok=True)
    adapter=load_adapter()
    core=adapter.install(adapter.load_core())
    stocks=core.load_stocks(core.STOCKS_FILE)
    results=[]
    for batch_size in CANDIDATES:
        results.append(run_candidate(core,stocks,batch_size))
        time.sleep(2)
    ranked=sorted(results,key=lambda r:(-r['success_rate'],r['total_request_errors'],r['max_elapsed_seconds'],r['batch_size']))
    best=ranked[0] if ranked else None
    summary={
        'component':'A4_001W2',
        'version':VERSION,
        'checked_at':datetime.now(TZ).isoformat(),
        'requested_symbols':len(stocks),
        'cycles_per_candidate':CYCLES_PER_CANDIDATE,
        'interval_seconds':INTERVAL_SECONDS,
        'results':results,
        'best':best,
        'candidate_ready':bool(best and best['pass']),
        'pass':bool(best and best['pass']),
    }
    (OUT/'summary.json').write_text(json.dumps(summary,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(summary,ensure_ascii=False))
    return 0 if summary['pass'] else 2


if __name__=='__main__':
    raise SystemExit(main())
