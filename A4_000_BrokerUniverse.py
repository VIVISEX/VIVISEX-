from __future__ import annotations
import argparse,csv,json,os,sys
from collections import defaultdict
from datetime import date,datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ=ZoneInfo('Asia/Taipei')
VERSION='A4_000_UNIVERSE_V1.1.0'
DEFAULT_SHEET='1MOs8_soYSsq8-UIFbDhYnBAogFX8cVSwZ-CCejNE0qM'
DEFAULT_TAB='10_券商原始資料'
OUTDIR=Path(os.getenv('A4_UNIVERSE_DIR','output/universe'))

# Real provider layout observed in production sheet 2026-08-18.
# Headers from B onward are legacy/misaligned. Positional schema below is authoritative
# until central repairs the source schema.
COL_DATE=0
COL_BROKER_CODE=1
COL_BROKER_NAME=2
COL_BRANCH_CODE=3
COL_STOCK_CODE=4
COL_STOCK_NAME=5
COL_BUY=6
COL_SELL=7
COL_NET=8
COL_TOTAL=9
COL_SOURCE_NAME=10
COL_SOURCE_DATE=11
COL_BATCH_ID=12
COL_DQ_STATUS=13
MIN_COLS=14


def _date(v:Any)->date:
    s=str(v).strip().split()[0]
    for fmt in ('%m/%d/%Y','%Y-%m-%d','%Y/%m/%d'):
        try:return datetime.strptime(s,fmt).date()
        except ValueError:pass
    raise ValueError(f'bad date={v}')

def _num(v:Any)->float:
    s=str(v).strip().replace(',','')
    return float(s) if s else 0.0

def _valid_code(code:str)->bool:
    c=code.strip().upper()
    return bool(c) and len(c)<=12 and c.replace('-','').isalnum()

def validate_layout(rows:list[list[Any]])->dict[str,Any]:
    if not rows or len(rows[0])<MIN_COLS: raise RuntimeError('broker sheet header/width invalid')
    header=[str(x).strip() for x in rows[0][:MIN_COLS]]
    required_positions={0:'資料日期',4:'股票代號',5:'股票名稱',6:'買進股數',7:'賣出股數',8:'買賣超股數'}
    position_checks={f'col_{i}_{name}': header[i]==name for i,name in required_positions.items()}
    # Explicitly detect the known legacy mismatch: B is labeled 市場別 but contains broker codes.
    sample_broker_codes=[]
    for r in rows[1:21]:
        if len(r)>=MIN_COLS and str(r[COL_BROKER_CODE]).strip(): sample_broker_codes.append(str(r[COL_BROKER_CODE]).strip())
    legacy_header_misaligned=(header[1]=='市場別' and any(x in {'MS','ML','MQ','JP','GS','NO','UB','BNP','CITI','WL'} for x in sample_broker_codes))
    return {'position_checks':position_checks,'legacy_header_misaligned':legacy_header_misaligned,'pass':all(position_checks.values())}

def build(rows:list[list[Any]],target:date)->tuple[list[dict[str,Any]],dict[str,Any]]:
    layout=validate_layout(rows)
    if not layout['pass']: raise RuntimeError('broker sheet positional schema changed')
    parsed=[]; bad_rows=0
    for rownum,r in enumerate(rows[1:],start=2):
        if len(r)<MIN_COLS: continue
        try:
            d=_date(r[COL_DATE]); code=str(r[COL_STOCK_CODE]).strip().upper()
            if d>=target or not _valid_code(code): continue
            dq=str(r[COL_DQ_STATUS]).strip()
            if dq and dq not in {'通過','PASS','OK'}: continue
            parsed.append((d,rownum,r,code))
        except Exception: bad_rows+=1
    if not parsed: raise RuntimeError('no valid prior-date broker rows')
    source_date=max(x[0] for x in parsed)
    agg=defaultdict(lambda:{'buy':0.0,'sell':0.0,'net':0.0,'name':'','brokers':set(),'batches':set()})
    selected_rows=0
    for d,rownum,r,code in parsed:
        if d!=source_date: continue
        selected_rows+=1; a=agg[code]
        a['name']=str(r[COL_STOCK_NAME]).strip(); a['brokers'].add(str(r[COL_BROKER_CODE]).strip()); a['batches'].add(str(r[COL_BATCH_ID]).strip())
        a['buy']+=_num(r[COL_BUY]); a['sell']+=_num(r[COL_SELL]); a['net']+=_num(r[COL_NET])
    universe=[]
    for code,a in agg.items():
        universe.append({'code':code,'name':a['name'],'source_date':source_date.isoformat(),'broker_buy':a['buy'],'broker_sell':a['sell'],'broker_net':a['net'],'broker_count':len(a['brokers']),'market':'UNRESOLVED'})
    universe.sort(key=lambda x:(-abs(x['broker_net']),x['code']))
    report={'component':'A4_000','version':VERSION,'target_date':target.isoformat(),'source_date':source_date.isoformat(),'source_rows':selected_rows,'symbols':len(universe),'duplicate_free':len({x['code'] for x in universe})==len(universe),'same_day_blocked':source_date<target,'bad_rows_skipped':bad_rows,'layout':layout,'market_resolution':'DEFER_TO_OFFICIAL_MASTER_OR_SHIOAJI_CONTRACT','pass':bool(universe) and source_date<target}
    return universe,report

def fetch_sheet()->list[list[Any]]:
    raw=os.getenv('GOOGLE_SERVICE_ACCOUNT_JSON','').strip()
    if not raw: raise RuntimeError('GOOGLE_SERVICE_ACCOUNT_JSON missing')
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build as gbuild
    info=json.loads(raw)
    creds=Credentials.from_service_account_info(info,scopes=['https://www.googleapis.com/auth/spreadsheets.readonly'])
    svc=gbuild('sheets','v4',credentials=creds,cache_discovery=False)
    sid=os.getenv('BROKER_SHEET_ID',DEFAULT_SHEET); tab=os.getenv('BROKER_SHEET_TAB',DEFAULT_TAB)
    return svc.spreadsheets().values().get(spreadsheetId=sid,range=f"'{tab}'!A:N",majorDimension='ROWS').execute().get('values',[])

def write(universe,report):
    OUTDIR.mkdir(parents=True,exist_ok=True)
    fields=['market','code','name','source_date','broker_buy','broker_sell','broker_net','broker_count']
    with (OUTDIR/'stocks.csv').open('w',encoding='utf-8',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows([{k:r.get(k) for k in fields} for r in universe])
    (OUTDIR/'universe.json').write_text(json.dumps({'report':report,'symbols':universe},ensure_ascii=False,indent=2),encoding='utf-8')

def selftest()->int:
    rows=[['資料日期','市場別','券商代號','券商名稱','股票代號','股票名稱','買進股數','賣出股數','買賣超股數','買進金額','賣出金額','買賣超金額','資料來源代號','資料來源網址'],['8/17/2026','MS','台灣摩根士丹利','1470','2330','台積電','100','20','80','120','src','8/17/2026','B1','通過'],['8/17/2026','ML','美林','1440','2330','台積電','50','10','40','60','src','8/17/2026','B1','通過'],['8/17/2026','GS','高盛','1480','6488','環球晶','0','30','-30','30','src','8/17/2026','B1','通過'],['8/18/2026','JP','摩根大通','8440','2317','鴻海','999','0','999','999','src','8/18/2026','B2','通過']]
    uni,rep=build(rows,date(2026,8,18)); checks={'dedup':len(uni)==2,'no_lookahead':all(x['source_date']=='2026-08-17' for x in uni),'aggregate':next(x for x in uni if x['code']=='2330')['broker_net']==120,'broker_count':next(x for x in uni if x['code']=='2330')['broker_count']==2,'market_not_guessed':all(x['market']=='UNRESOLVED' for x in uni),'legacy_mismatch_detected':rep['layout']['legacy_header_misaligned']}
    rep['checks']=checks;rep['pass']=all(checks.values());write(uni,rep);print(json.dumps(rep,ensure_ascii=False));return 0 if rep['pass'] else 2

def main()->int:
    p=argparse.ArgumentParser();p.add_argument('--self-test',action='store_true');p.add_argument('--target-date');a=p.parse_args()
    if a.self_test:return selftest()
    target=_date(a.target_date) if a.target_date else datetime.now(TZ).date(); uni,rep=build(fetch_sheet(),target);write(uni,rep);print(json.dumps(rep,ensure_ascii=False));return 0 if rep['pass'] else 3
if __name__=='__main__':
    try:sys.exit(main())
    except Exception as e:
        OUTDIR.mkdir(parents=True,exist_ok=True);err={'component':'A4_000','version':VERSION,'pass':False,'error':f'{type(e).__name__}: {e}'};(OUTDIR/'failure.json').write_text(json.dumps(err,ensure_ascii=False,indent=2),encoding='utf-8');print(json.dumps(err,ensure_ascii=False),file=sys.stderr);sys.exit(99)
