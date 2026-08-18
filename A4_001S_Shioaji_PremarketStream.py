from __future__ import annotations
import csv,json,os,sys,time,threading
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
TZ=ZoneInfo('Asia/Taipei'); VERSION='A4_001S_STREAM_V1.0.0'
UNIVERSE=Path(os.getenv('A4_UNIVERSE_FILE','output/universe/stocks.csv')); OUT=Path(os.getenv('SHIOAJI_OUTPUT_DIR','output/shioaji'))
MAX_CONN=int(os.getenv('SJ_MAX_CONNECTIONS','5')); SUBS_PER_CONN=int(os.getenv('SJ_SUBS_PER_CONNECTION','200')); GAP_SEC=float(os.getenv('SJ_SOURCE_GAP_SECONDS','30'))

def now(): return datetime.now(TZ)
def load_symbols():
    if not UNIVERSE.exists(): raise RuntimeError(f'universe missing: {UNIVERSE}')
    out=[];seen=set()
    with UNIVERSE.open(encoding='utf-8-sig',newline='') as f:
        for r in csv.DictReader(f):
            c=str(r.get('code','')).strip(); m=str(r.get('market','')).strip().upper()
            if not c or c in seen: continue
            if m not in {'TSE','OTC'}: raise RuntimeError(f'bad market {m}:{c}')
            seen.add(c);out.append((m,c))
    if not out: raise RuntimeError('empty universe')
    if len(out)>MAX_CONN*SUBS_PER_CONN: raise RuntimeError(f'universe {len(out)} exceeds configured shioaji capacity {MAX_CONN*SUBS_PER_CONN}')
    return out

def norm(exchange:Any,p:Any):
    def g(n,d=None): return p.get(n,d) if isinstance(p,dict) else getattr(p,n,d)
    def l5(n):
        v=list(g(n,[]) or [])[:5]; return [str(x) if x is not None else None for x in v]+[None]*(5-len(v))
    rec={'source':'SHIOAJI','captured_at':now().isoformat(timespec='milliseconds'),'exchange':str(getattr(exchange,'value',exchange)),'code':str(g('code','')),'market_date':str(g('date','')),'market_time':str(g('time','')),'simtrade':bool(g('simtrade',False)),'suspend':bool(g('suspend',False))}
    for k in ('bid_price','bid_volume','ask_price','ask_volume','diff_bid_vol','diff_ask_vol'):
        vals=l5(k)
        for i,v in enumerate(vals,1): rec[f'{k}_{i}']=v
    return rec

def selftest():
    OUT.mkdir(parents=True,exist_ok=True)
    sample={'code':'2330','date':'2026-08-18','time':'08:45:00','bid_price':[1,0.9,0.8,0.7,0.6],'bid_volume':[1,2,3,4,5],'ask_price':[1.1,1.2,1.3,1.4,1.5],'ask_volume':[6,7,8,9,10],'diff_bid_vol':[1,1,1,1,1],'diff_ask_vol':[-1,-1,-1,-1,-1],'simtrade':True}
    r=norm('TSE',sample); checks={'simtrade':r['simtrade'] is True,'five_bid':r['bid_price_5']=='0.6','five_ask':r['ask_volume_5']=='10','diff':r['diff_ask_vol_1']=='-1'}
    rep={'component':'A4_001S','version':VERSION,'mode':'selftest','checks':checks,'pass':all(checks.values())};(OUT/'selftest.json').write_text(json.dumps(rep,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(rep,ensure_ascii=False));return 0 if rep['pass'] else 2

def live(seconds:int):
    try: import shioaji as sj
    except Exception as e: raise RuntimeError(f'shioaji import failed: {e}')
    key=os.getenv('SJ_API_KEY','').strip(); sec=os.getenv('SJ_SEC_KEY','').strip()
    if not key or not sec: raise RuntimeError('SJ_API_KEY/SJ_SEC_KEY missing')
    syms=load_symbols(); chunks=[syms[i:i+SUBS_PER_CONN] for i in range(0,len(syms),SUBS_PER_CONN)]
    OUT.mkdir(parents=True,exist_ok=True); stamp=now().strftime('%Y%m%d_%H%M%S'); raw=OUT/f'shioaji_{stamp}.ndjson'; report=OUT/f'shioaji_{stamp}_report.json'
    lock=threading.Lock(); last_event=time.monotonic(); seen=set(); counts=0; sim=0; five=0; apis=[]
    fh=raw.open('a',encoding='utf-8')
    def persist(ex,p):
        nonlocal last_event,counts,sim,five
        r=norm(ex,p)
        with lock:
            fh.write(json.dumps(r,ensure_ascii=False)+'\n');fh.flush();last_event=time.monotonic();counts+=1
            if r['simtrade']: sim+=1
            if r['code']: seen.add(r['code'])
            if all(r.get(f'{s}_{k}_{i}') is not None for s in ('bid','ask') for k in ('price','volume') for i in range(1,6)): five+=1
    try:
        for chunk in chunks:
            api=sj.Shioaji(simulation=False); api.login(api_key=key,secret_key=sec,subscribe_trade=False); api.set_on_bidask_stk_v1_callback(persist); apis.append(api)
            for m,c in chunk:
                ct=api.contracts.get(c)
                if ct is None: raise RuntimeError(f'contract not found:{c}')
                api.subscribe(ct,quote_type=sj.QuoteType.BidAsk)
        deadline=time.monotonic()+max(1,seconds); gap=False
        while time.monotonic()<deadline:
            if counts>0 and time.monotonic()-last_event>GAP_SEC: gap=True
            time.sleep(.2)
    finally:
        for api in apis:
            try: api.logout()
            except Exception: pass
        fh.close()
    rep={'component':'A4_001S','version':VERSION,'mode':'live','started_at':stamp,'symbols':len(syms),'connections':len(chunks),'events':counts,'seen_symbols':len(seen),'coverage':len(seen)/len(syms) if syms else 0,'simtrade_events':sim,'five_level_events':five,'source_gap':gap,'pass':counts>0 and five>0 and not gap}
    report.write_text(json.dumps(rep,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(rep,ensure_ascii=False));return 0 if rep['pass'] else 3

def main():
    import argparse;p=argparse.ArgumentParser();p.add_argument('--self-test',action='store_true');p.add_argument('--seconds',type=int,default=30);a=p.parse_args();return selftest() if a.self_test else live(a.seconds)
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as e:
        OUT.mkdir(parents=True,exist_ok=True);err={'component':'A4_001S','version':VERSION,'pass':False,'error':f'{type(e).__name__}: {e}'};(OUT/'failure.json').write_text(json.dumps(err,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(err,ensure_ascii=False),file=sys.stderr);sys.exit(99)
