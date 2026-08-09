from __future__ import annotations
import json,time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'output'; OUT.mkdir(parents=True,exist_ok=True)
URL='https://5850web.moneydj.com/z/zc/zco/zco0/zco0.djhtm?a=2330&b=1470'

def drv():
 o=Options(); o.add_argument('--headless=new'); o.add_argument('--no-sandbox'); o.add_argument('--disable-dev-shm-usage'); o.add_argument('--window-size=1920,1080'); return webdriver.Chrome(options=o)

def snapshot(d,label):
 body=d.find_element(By.TAG_NAME,'body').text
 return {'label':label,'url':d.current_url,'title':d.title,'body_tail':body[-5000:]}

def main():
 d=drv(); out={'version':'A2_011_INTERACTION_PROBE_V1.0.0','snapshots':[],'errors':[]}
 try:
  d.get(URL); time.sleep(3); out['snapshots'].append(snapshot(d,'initial'))
  # enumerate relevant form fields
  fields=d.execute_script("""
   const f=document.forms['F']; if(!f) return [];
   return Array.from(f.elements).map(e=>({name:e.name,type:e.type,value:e.value,tag:e.tagName,options:e.options?Array.from(e.options).map(o=>({v:o.value,t:o.text,sel:o.selected})):null}));
  """)
  out['fields']=fields
  # Exact UI path: choose MS broker, custom range, inject old year options if the UI hides them, then call page JS.
  js="""
   const f=document.forms['F'];
   function ensure(sel,val){ if(!sel) return; let hit=Array.from(sel.options).some(o=>o.value==String(val)); if(!hit){ let o=new Option(String(val),String(val)); sel.add(o);} sel.value=String(val); }
   if(f.sel_Broker) f.sel_Broker.value='1470';
   if(f.sel_BrokerBranch) f.sel_BrokerBranch.value='1470';
   if(f.D) f.D.value='0';
   ensure(f.Y1,2022); ensure(f.M1,1); ensure(f.D1,3);
   ensure(f.Y2,2025); ensure(f.M2,6); ensure(f.D2,30);
   return {hasGoA:typeof goA==='function',hasGoPage:typeof GoPage==='function',action:f.action,method:f.method,
     values:Array.from(f.elements).filter(e=>['D','Y1','M1','D1','Y2','M2','D2','sel_Broker','sel_BrokerBranch'].includes(e.name)).map(e=>[e.name,e.value])};
  """
  prep=d.execute_script(js); out['prep']=prep
  try:
   d.execute_script("if(typeof goA==='function'){goA();} else if(typeof GoPage==='function'){GoPage(document.forms['F']);}")
   time.sleep(5); out['snapshots'].append(snapshot(d,'after_goA'))
  except Exception as e: out['errors'].append('goA:'+repr(e))
  # Second path: construct URL from actual form field names after UI prep to expose query semantics.
  d.get(URL); time.sleep(2)
  q=d.execute_script("""
   const f=document.forms['F'];
   const names=['D','Y1','M1','D1','Y2','M2','D2','sel_Broker','sel_BrokerBranch'];
   const x={}; for(const n of names){if(f[n]) x[n]=f[n].value;} return x;
  """)
  out['current_defaults']=q
  (OUT/'report.json').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
  print(json.dumps({'prep':out.get('prep'),'snapshots':[{'label':x['label'],'url':x['url']} for x in out['snapshots']]},ensure_ascii=False))
 finally:d.quit()
if __name__=='__main__': raise SystemExit(main())
