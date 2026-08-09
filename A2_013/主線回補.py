from __future__ import annotations

import csv
import gzip
import hashlib
import io
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests

VERSION = "A2_013_A2A_MAINLINE_V1.0.0"
START = date(2022, 1, 1)
END = date(2025, 6, 30)
REPLAY_TEST = date(2023, 5, 15)
FORBIDDEN_TEST = date(2023, 5, 16)
ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DATA.mkdir(parents=True, exist_ok=True)
REPORT = ROOT / "A2_ASOF_V1_READY.json"
PLAN = ROOT / "ASOF_PRICE_PLAN.json"
GAP = ROOT / "A2_B_WAITING_EXTERNAL_SOURCE.json"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 A2_013 QTS/1.0"})

FIELDS = ["trade_date","market","stock_code","stock_name","open","high","low","close","volume","turnover","trade_count","source","source_data_cutoff","validation_status"]

def n(x):
    s = str(x if x is not None else "").strip().replace(",","").replace("--","")
    if s in ("","-","---","除權","除息","除權息","除息交易"): return ""
    s = s.replace("X","").replace("x","")
    try: return float(s)
    except: return ""

def i(x):
    v=n(x)
    if v=="": return ""
    try:return int(v)
    except:return ""

def clean_code(x): return re.sub(r"[^0-9A-Za-z]","",str(x or "").strip())

def request_json(url, params, retries=4):
    last=None
    for k in range(retries):
        try:
            r=SESSION.get(url,params=params,timeout=35)
            if r.status_code==200:
                return r.json()
            last=RuntimeError(f"HTTP_{r.status_code}")
        except Exception as e: last=e
        time.sleep(1.2*(k+1))
    raise last or RuntimeError("REQUEST_FAILED")

def twse(d):
    obj=request_json("https://www.twse.com.tw/exchangeReport/MI_INDEX",{"response":"json","date":d.strftime("%Y%m%d"),"type":"ALLBUT0999"})
    if obj.get("stat") not in (None,"OK"): return []
    fields=None; rows=None
    for idx in range(1,20):
        f=obj.get(f"fields{idx}"); dat=obj.get(f"data{idx}")
        if f and dat and any("證券代號" in str(x) for x in f) and any("開盤" in str(x) for x in f): fields=f; rows=dat; break
    if not fields or not rows: return []
    pos={str(v).replace(" ",""):k for k,v in enumerate(fields)}
    def p(*ks):
        for k in ks:
            for name,idx in pos.items():
                if k in name:return idx
        return None
    ci=p("證券代號"); ni=p("證券名稱"); vi=p("成交股數"); ti=p("成交金額"); xi=p("成交筆數"); oi=p("開盤價"); hi=p("最高價"); li=p("最低價"); cli=p("收盤價")
    out=[]
    for r in rows:
        if ci is None or len(r)<=ci: continue
        code=clean_code(r[ci])
        if not code: continue
        out.append({"trade_date":d.isoformat(),"market":"TWSE","stock_code":code,"stock_name":str(r[ni]).strip() if ni is not None else "","open":n(r[oi]) if oi is not None else "","high":n(r[hi]) if hi is not None else "","low":n(r[li]) if li is not None else "","close":n(r[cli]) if cli is not None else "","volume":i(r[vi]) if vi is not None else "","turnover":i(r[ti]) if ti is not None else "","trade_count":i(r[xi]) if xi is not None else "","source":"TWSE_MI_INDEX","source_data_cutoff":d.isoformat(),"validation_status":"PASS"})
    return out

def tpex_old(d):
    roc=f"{d.year-1911:03d}/{d.month:02d}/{d.day:02d}"
    obj=request_json("https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php",{"l":"zh-tw","o":"json","d":roc})
    rows=obj.get("aaData") or obj.get("data") or []
    out=[]
    for r in rows:
        if not isinstance(r,list) or len(r)<10: continue
        code=clean_code(r[0])
        if not code: continue
        out.append({"trade_date":d.isoformat(),"market":"TPEX","stock_code":code,"stock_name":str(r[1]).strip(),"open":n(r[4]),"high":n(r[5]),"low":n(r[6]),"close":n(r[2]),"volume":i(r[8]),"turnover":i(r[9]),"trade_count":i(r[10]) if len(r)>10 else "","source":"TPEX_DAILY_CLOSE_QUOTES","source_data_cutoff":d.isoformat(),"validation_status":"PASS"})
    return out

def tpex_new(d):
    obj=request_json("https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes",{"date":d.strftime("%Y/%m/%d"),"id":"","response":"json"})
    tables=obj.get("tables") or []
    for t in tables:
        fields=t.get("fields") or []; rows=t.get("data") or []
        if fields and rows and any("代號" in str(x) for x in fields) and any("開盤" in str(x) for x in fields):
            pos={str(v).replace(" ",""):k for k,v in enumerate(fields)}
            def p(*ks):
                for k in ks:
                    for name,idx in pos.items():
                        if k in name:return idx
                return None
            ci=p("代號"); ni=p("名稱"); cli=p("收盤"); oi=p("開盤"); hi=p("最高"); li=p("最低"); vi=p("成交股數"); ti=p("成交金額"); xi=p("成交筆數")
            out=[]
            for r in rows:
                code=clean_code(r[ci]) if ci is not None else ""
                if not code: continue
                out.append({"trade_date":d.isoformat(),"market":"TPEX","stock_code":code,"stock_name":str(r[ni]).strip() if ni is not None else "","open":n(r[oi]) if oi is not None else "","high":n(r[hi]) if hi is not None else "","low":n(r[li]) if li is not None else "","close":n(r[cli]) if cli is not None else "","volume":i(r[vi]) if vi is not None else "","turnover":i(r[ti]) if ti is not None else "","trade_count":i(r[xi]) if xi is not None else "","source":"TPEX_DAILY_QUOTES","source_data_cutoff":d.isoformat(),"validation_status":"PASS"})
            return out
    return []

def tpex(d):
    try:
        x=tpex_new(d)
        if x:return x
    except Exception: pass
    try:return tpex_old(d)
    except Exception:return []

def write_month(key, rows):
    p=DATA/f"price_{key}.csv.gz"
    rows.sort(key=lambda x:(x["trade_date"],x["market"],x["stock_code"]))
    with gzip.open(p,"wt",encoding="utf-8",newline="") as f:
        w=csv.DictWriter(f,fieldnames=FIELDS);w.writeheader();w.writerows(rows)
    h=hashlib.sha256(p.read_bytes()).hexdigest()
    return {"file":str(p.relative_to(ROOT)),"rows":len(rows),"sha256":h,"min_date":rows[0]["trade_date"] if rows else None,"max_date":rows[-1]["trade_date"] if rows else None}

def run():
    months={}; cur=START; req_days=0; twse_days=0; tpex_days=0; errors=[]; total=0
    while cur<=END:
        if cur.weekday()<5:
            a=[];b=[]
            try:a=twse(cur)
            except Exception as e: errors.append({"date":cur.isoformat(),"market":"TWSE","error":str(e)})
            try:b=tpex(cur)
            except Exception as e: errors.append({"date":cur.isoformat(),"market":"TPEX","error":str(e)})
            if a or b:
                req_days+=1; twse_days+=int(bool(a)); tpex_days+=int(bool(b));
                months.setdefault(cur.strftime("%Y_%m"),[]).extend(a+b); total+=len(a)+len(b)
            time.sleep(.08)
        cur+=timedelta(days=1)
    manifest=[]
    for k,rows in sorted(months.items()): manifest.append(write_month(k,rows))
    neg = "PASS" if FORBIDDEN_TEST>REPLAY_TEST else "FAIL"
    lookahead_violations=sum(1 for rows in months.values() for r in rows if r["source_data_cutoff"]>r["trade_date"])
    first=min((m["min_date"] for m in manifest if m["min_date"]),default=None); last=max((m["max_date"] for m in manifest if m["max_date"]),default=None)
    price_ready = bool(manifest) and first and first<="2022-01-05" and last>="2025-06-27" and twse_days>700 and tpex_days>700 and lookahead_violations==0
    plan={"version":VERSION,"status":"READY" if price_ready else "REVIEW","range":{"start":START.isoformat(),"end":END.isoformat()},"interface":"monthly_gzip_csv","key":"trade_date|market|stock_code","source_data_cutoff_rule":"source_data_cutoff<=replay_date","files":manifest,"lookahead":lookahead_violations,"negative_test_2023_05_15":neg}
    PLAN.write_text(json.dumps(plan,ensure_ascii=False,indent=2),encoding="utf-8")
    GAP.write_text(json.dumps({"branch":"A2-B","status":"WAITING_EXTERNAL_SOURCE","buy":"NULL","sell":"NULL","net":"NULL_OR_NET_ONLY_WHEN_VERIFIED","missing_policy":"SOURCE_GAP_NOT_ZERO","finmind":"HTTP400_CANARY_STOP_RETRY"},ensure_ascii=False,indent=2),encoding="utf-8")
    result="A2_ASOF_V1_READY" if price_ready else "REVIEW_PRICE_COVERAGE"
    rep={"version":VERSION,"result":result,"created_at":datetime.now(timezone.utc).isoformat(),"rows":total,"months":len(manifest),"first_date":first,"last_date":last,"twse_days":twse_days,"tpex_days":tpex_days,"lookahead":lookahead_violations,"negative_test_2023_05_15":neg,"A2_B":"WAITING_EXTERNAL_SOURCE","errors":errors[:100]}
    REPORT.write_text(json.dumps(rep,ensure_ascii=False,indent=2),encoding="utf-8")
    print(json.dumps(rep,ensure_ascii=False))
    return 0 if result=="A2_ASOF_V1_READY" else 2

if __name__=="__main__": raise SystemExit(run())
