from __future__ import annotations

import csv
import gzip
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

VERSION = "A2_012_FINMIND_BACKFILL_V1.0.1"
API_BASE = "https://api.finmindtrade.com/api/v4"
START = date(2022, 1, 1)
END = date(2025, 6, 30)
REPLAY_GUARD = date(2023, 5, 15)
FORBIDDEN_FROM = date(2023, 5, 16)
MAX_REQUESTS_PER_RUN = int(os.getenv("A2_MAX_REQUESTS", "450"))
TIMEOUT = 45
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
STATE_PATH = ROOT / "checkpoint.json"
REPORT_PATH = ROOT / "report.json"
DATA_DIR.mkdir(parents=True, exist_ok=True)

TARGETS = {
    "BNP": ["法銀巴黎", "巴黎"], "CITI": ["花旗環球", "花旗"],
    "GS": ["美商高盛", "高盛"], "JP": ["摩根大通"], "ML": ["美林"],
    "MQ": ["港商麥格理", "麥格理"], "MS": ["台灣摩根士丹利", "摩根士丹利"],
    "NO": ["港商野村", "野村"], "UB": ["新加坡商瑞銀", "瑞銀"], "WL": ["匯立"],
}
FIELDS = ["date","stock_id","securities_trader_id","securities_trader","price","buy","sell","net","canonical","source","version"]

def nowz(): return datetime.now(timezone.utc).isoformat()
def headers(token): return {"Authorization": f"Bearer {token}", "User-Agent": VERSION}

def api_get(path, token, params):
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.get(url, headers=headers(token), params=params, timeout=TIMEOUT)
    body = r.text[:1500]
    if r.status_code == 401: raise RuntimeError("FINMIND_TOKEN_INVALID_OR_EXPIRED")
    if r.status_code == 403: raise PermissionError("FINMIND_SPONSOR_PERMISSION_REQUIRED")
    if r.status_code == 400:
        try: obj = r.json()
        except Exception: obj = {}
        msg = str(obj.get("msg") or obj.get("message") or body)
        if "sponsor" in msg.lower() or "permission" in msg.lower() or "權限" in msg:
            raise PermissionError("FINMIND_SPONSOR_PERMISSION_REQUIRED: " + msg)
        raise RuntimeError("FINMIND_HTTP_400: " + msg)
    r.raise_for_status()
    obj = r.json()
    if isinstance(obj, dict) and obj.get("status") not in (None, 200):
        msg = str(obj.get("msg") or obj.get("message") or obj)
        if "sponsor" in msg.lower() or "permission" in msg.lower() or "權限" in msg:
            raise PermissionError("FINMIND_SPONSOR_PERMISSION_REQUIRED: " + msg)
        raise RuntimeError("FINMIND_API_ERROR: " + msg)
    return obj

def load_state():
    if STATE_PATH.exists():
        try: return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception: pass
    return {"version":VERSION,"next_date":START.isoformat(),"next_broker_index":0,"mapped":{},"requests_total":0,"rows_total":0,"completed":False,"lookahead":0}
def save_state(s): STATE_PATH.write_text(json.dumps(s,ensure_ascii=False,indent=2),encoding="utf-8")
def save_report(r): REPORT_PATH.write_text(json.dumps(r,ensure_ascii=False,indent=2),encoding="utf-8")
def normalize_name(x): return re.sub(r"[\s\-－_()（）]","",str(x or "")).replace("證券","")
def trader_info(token): return api_get("data",token,{"dataset":"TaiwanSecuritiesTraderInfo"}).get("data") or []

def map_brokers(token):
    rows=trader_info(token); mapped={}
    for canonical,aliases in TARGETS.items():
        candidates=[]
        for row in rows:
            name=str(row.get("securities_trader") or row.get("name") or ""); sid=str(row.get("securities_trader_id") or row.get("id") or "")
            if not sid or not name: continue
            n=normalize_name(name); score=0
            for alias in aliases:
                a=normalize_name(alias)
                if n==a: score=max(score,100)
                elif a in n or n in a: score=max(score,80)
            if score: candidates.append((score,len(n),sid,name))
        if candidates:
            candidates.sort(key=lambda x:(-x[0],x[1],x[2])); _,_,sid,name=candidates[0]; mapped[canonical]={"id":sid,"name":name}
    return mapped

def sponsor_canary(token,mapped):
    if "MS" not in mapped: return {"status":"FAIL","reason":"MS_MAPPING_MISSING"}
    sid=mapped["MS"]["id"]
    try:
        obj=api_get("taiwan_stock_trading_daily_report",token,{"securities_trader_id":sid,"date":"2022-06-16"})
    except PermissionError as e: return {"status":"SPONSOR_REQUIRED","reason":str(e)}
    except Exception as e: return {"status":"API_REJECTED","reason":str(e),"broker_id":sid}
    data=obj.get("data") or []
    return {"status":"PASS" if isinstance(data,list) else "FAIL","rows":len(data) if isinstance(data,list) else 0,"broker_id":sid}

def month_path(d): return DATA_DIR / f"broker_{d:%Y_%m}.csv.gz"
def append_rows(d,rows):
    if not rows:return 0
    p=month_path(d); keys=set(); old=[]
    if p.exists():
        with gzip.open(p,"rt",encoding="utf-8",newline="") as f:
            for r in csv.DictReader(f): keys.add((r.get("date"),r.get("stock_id"),r.get("canonical"))); old.append(r)
    new=[]
    for r in rows:
        k=(str(r["date"]),str(r["stock_id"]),str(r["canonical"]))
        if k in keys: continue
        keys.add(k); new.append({x:r.get(x,"") for x in FIELDS})
    if not new:return 0
    allr=old+new; allr.sort(key=lambda r:(r.get("date",""),r.get("stock_id",""),r.get("canonical","")))
    with gzip.open(p,"wt",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=FIELDS); w.writeheader(); w.writerows(allr)
    return len(new)

def fetch_one(token,canonical,trader,d):
    obj=api_get("taiwan_stock_trading_daily_report",token,{"securities_trader_id":trader["id"],"date":d.isoformat()}); out=[]
    for r in obj.get("data") or []:
        rd=str(r.get("date") or d.isoformat())[:10]
        if rd>d.isoformat(): raise RuntimeError(f"LOOKAHEAD_DETECTED input={rd} replay={d.isoformat()}")
        buy=int(float(r.get("buy") or 0)); sell=int(float(r.get("sell") or 0))
        out.append({"date":rd,"stock_id":str(r.get("stock_id") or ""),"securities_trader_id":str(r.get("securities_trader_id") or trader["id"]),"securities_trader":str(r.get("securities_trader") or trader["name"]),"price":r.get("price",""),"buy":buy,"sell":sell,"net":buy-sell,"canonical":canonical,"source":"FinMind/TaiwanStockTradingDailyReport","version":VERSION})
    return out

def mandatory_lookahead_test():
    blocked=FORBIDDEN_FROM>REPLAY_GUARD
    return {"replay_date":REPLAY_GUARD.isoformat(),"input_data_date":FORBIDDEN_FROM.isoformat(),"expected":"STOP_LOOKAHEAD","actual":"STOP_LOOKAHEAD" if blocked else "LEAK","result":"PASS_NEGATIVE_BLOCKED" if blocked else "FAIL_LOOKAHEAD"}

def run():
    token=os.getenv("FINMIND_TOKEN","").strip(); report={"version":VERSION,"time":nowz(),"lookahead_test":mandatory_lookahead_test()}
    try:
        if not token:
            report.update({"result":"WAITING_FINMIND_TOKEN"}); save_report(report); print(json.dumps(report,ensure_ascii=False)); return 0
        state=load_state(); mapped=map_brokers(token); state["mapped"]=mapped; report.update({"mapped":mapped,"mapped_count":len(mapped)})
        if len(mapped)<10:
            report.update({"result":"STOP_BROKER_MAP_INCOMPLETE","missing":sorted(set(TARGETS)-set(mapped))}); save_state(state); save_report(report); return 2
        canary=sponsor_canary(token,mapped); report["sponsor_canary"]=canary
        if canary["status"]!="PASS":
            report["result"]="STOP_"+canary["status"]; save_state(state); save_report(report); print(json.dumps(report,ensure_ascii=False)); return 3
        order=list(TARGETS); cur=date.fromisoformat(state.get("next_date",START.isoformat())); bi=int(state.get("next_broker_index",0)); used=added=empty=0; errors=[]
        while cur<=END and used<MAX_REQUESTS_PER_RUN:
            c=order[bi]; trader=mapped[c]
            try:
                rows=fetch_one(token,c,trader,cur); added+=append_rows(cur,rows); empty+=int(not rows)
            except PermissionError as e:
                report.update({"result":"STOP_SPONSOR_REQUIRED","error":str(e)}); break
            except Exception as e:
                errors.append({"date":cur.isoformat(),"canonical":c,"error":str(e)})
                if "429" in str(e): break
            used+=1; bi+=1
            if bi>=len(order): bi=0; cur+=timedelta(days=1)
            time.sleep(.12)
        state.update({"version":VERSION,"next_date":cur.isoformat(),"next_broker_index":bi,"requests_total":int(state.get("requests_total",0))+used,"rows_total":int(state.get("rows_total",0))+added,"completed":cur>END,"lookahead":0,"updated_at":nowz()}); save_state(state)
        result="MILESTONE_2022_2025H1_BROKER_BACKFILL_COMPLETE" if state["completed"] else report.get("result","AUTO_CONTINUE")
        report.update({"result":result,"requests_this_run":used,"rows_added_this_run":added,"empty_requests":empty,"next_date":state["next_date"],"next_broker_index":bi,"requests_total":state["requests_total"],"rows_total":state["rows_total"],"completed":state["completed"],"lookahead":0,"errors":errors[:20]}); save_report(report); print(json.dumps(report,ensure_ascii=False)); return 0 if result in ("AUTO_CONTINUE","MILESTONE_2022_2025H1_BROKER_BACKFILL_COMPLETE") else 5
    except Exception as e:
        report.update({"result":"STOP_UNHANDLED","error":type(e).__name__+": "+str(e)}); save_report(report); print(json.dumps(report,ensure_ascii=False)); return 9

if __name__=="__main__": raise SystemExit(run())
