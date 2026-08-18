from __future__ import annotations
import argparse,csv,json,os,sys
from collections import defaultdict
from datetime import date,datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo
TZ=ZoneInfo('Asia/Taipei'); VERSION='A4_000_UNIVERSE_V1.0.0'
DEFAULT_SHEET='1MOs8_soYSsq8-UIFbDhYnBAogFX8cVSwZ-CCejNE0qM'; DEFAULT_TAB='10_券商原始資料'
OUTDIR=Path(os.getenv('A4_UNIVERSE_DIR','output/universe'))

def _date(v:str)->date:
    s=str(v).strip().split()[0].replace('/','-')
    return datetime.strptime(s,'%Y-%m-%d').date()

def _market(v:str)->str:
    s=str(v).strip().upper()
    if s in {'TSE','TWSE','上市'}: return 'TSE'
    if s in {'OTC','TPEX','上櫃'}: return 'OTC'
    raise ValueError(f'unsupported market={v}')

def build(rows:list[list[Any]], target:date)->tuple[list[dict[str,Any]],dict[str,Any]]:
    if not rows: raise RuntimeError('empty sheet')
    hdr=[str(x).strip() for x in rows[0]]
    req=['資料日期','市場別','股票代號','股票名稱','買進股數','賣出股數','買賣超股數']
    miss=[x for x in req if x not in hdr]
    if miss: raise RuntimeError('missing headers:'+','.join(miss))
    idx={h:i for i,h in enumerate(hdr)}
    parsed=[]
    for r in rows[1:]:
        try:
            d=_date(r[idx['資料日期']]); code=str(r[idx['股票代號']]).strip()
            if not code or d>=target: continue
            parsed.append((d,r,code))
        except Exception: continue
    if not parsed: raise RuntimeError('no prior-date rows')
    source_date=max(x[0] for x in parsed)
    agg=defaultdict(lambda:{'buy':0.0,'sell':0.0,'net':0.0,'name':'','market':''})
    for d,r,code in parsed:
        if d!=source_date: continue
        a=agg[code]; a['name']=str(r[idx['股票名稱']]).strip(); a['market']=_market(r[idx['市場別']])
        for k,col in [('buy','買進股數'),('sell','賣出股數'),('net','買賣超股數')]:
            try:a[k]+=float(str(r[idx[col]]).replace(',','') or 0)
            except Exception: pass
    uni=[]
    for code,a in agg.items():
        uni.append({'market':a['market'],'code':code,'name':a['name'],'source_date':source_date.isoformat(),'broker_buy':a['buy'],'broker_sell':a['sell'],'broker_net':a['net']})
    uni.sort(key=lambda x:(-abs(x['broker_net']),x['code']))
    rep={'component':'A4_000','version':VERSION,'target_date':target.isoformat(),'source_date':source_date.isoformat(),'symbols':len(uni),'duplicate_free':len({x['code'] for x in uni})==len(uni),'same_day_blocked':source_date<target,'pass':bool(uni)}
    return uni,rep

def fetch_sheet()->list[list[Any]]:
    raw=os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON','').strip()
    if not raw: raise RuntimeError('GOOGLE_SERVICE_ACCOUNT_JSON missing')
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build as gbuild
    info=json.loads(raw); creds=Credentials.from_service_account_info(info,scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    svc=gbuild('sheets','v4',credentials=creds,cache_discovery=False)
    sid=os.getenv('BROKER_SHEET_ID',DEFAULT_SHEET); tab=os.getenv('BROKER_SHEET_TAB',DEFAULT_TAB)
    return svc.spreadsheets().values().get(spreadsheetId=sid,range=f"'{tab}'!A:N",majorDimension='ROWS').execute().get('values',[])

def write(uni,rep):
    OUTDIR.mkdir(parents=True,exist_ok=True)
    with (OUTDIR/'stocks.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=['market','code','name','source_date','broker_buy','broker_sell','broker_net']);w.writeheader();w.writerows(uni)
    (OUTDIR/'universe.json').write_text(json.dumps({'report':rep,'symbols':uni},ensure_ascii=False,indent=2),encoding='utf-8')

def selftest()->int:
    rows=[['資料日期','市場別','券商代號','券商名稱','股票代號','股票名稱','買進股數','賣出股數','買賣超股數'],['2026-08-17','上市','A','X','2330','台積電','100','20','80'],['2026-08-17','上市','B','Y','2330','台積電','50','10','40'],['2026-08-17','上櫃','A','X','6488','環球晶','0','30','-30'],['2026-08-18','上市','A','X','2317','鴻海','999','0','999']]
    uni,rep=build(rows,date(2026,8,18)); checks={'dedup':len(uni)==2,'no_lookahead':all(x['source_date']=='2026-08-17' for x in uni),'aggregate':next(x for x in uni if x['code']=='2330')['broker_net']==120,'market':next(x for x in uni if x['code']=='6488')['market']=='OTC'}
    rep['checks']=checks;rep['pass']=all(checks.values());write(uni,rep);print(json.dumps(rep,ensure_ascii=False));return 0 if rep['pass'] else 2

def main()->int:
    p=argparse.ArgumentParser();p.add_argument('--self-test',action='store_true');p.add_argument('--target-date');a=p.parse_args()
    if a.self_test:return selftest()
    target=_date(a.target_date) if a.target_date else datetime.now(TZ).date(); uni,rep=build(fetch_sheet(),target);write(uni,rep);print(json.dumps(rep,ensure_ascii=False));return 0 if rep['pass'] else 3
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as e:
        OUTDIR.mkdir(parents=True,exist_ok=True); err={'component':'A4_000','version':VERSION,'pass':False,'error':f'{type(e).__name__}: {e}'};(OUTDIR/'failure.json').write_text(json.dumps(err,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(err,ensure_ascii=False),file=sys.stderr);sys.exit(99)
