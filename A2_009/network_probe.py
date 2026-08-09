from __future__ import annotations
import json, time
from pathlib import Path
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'output'; OUT.mkdir(parents=True,exist_ok=True)
TARGET='https://5850web.moneydj.com/z/zc/zco/zco0/zco0.djhtm?a=2330&A=2330&BHID=1470&b=1470&C=1&D=2022-01-01&E=2025-06-30&ver=V3'

def driver_():
 o=Options(); o.add_argument('--headless=new'); o.add_argument('--no-sandbox'); o.add_argument('--disable-dev-shm-usage'); o.add_argument('--window-size=1920,1080'); o.add_argument('--lang=zh-TW'); o.set_capability('goog:loggingPrefs',{'performance':'ALL','browser':'ALL'})
 d=webdriver.Chrome(options=o); d.set_page_load_timeout(60); return d

def main():
 d=driver_(); events=[]
 try:
  d.get(TARGET); time.sleep(8)
  for x in d.get_log('performance'):
   try:
    m=json.loads(x['message'])['message']; p=m.get('params',{}); method=m.get('method','')
    if method=='Network.responseReceived':
     r=p.get('response',{}); u=r.get('url','')
     if u.startswith('http'): events.append({'url':u,'status':r.get('status'),'mime':r.get('mimeType'),'type':p.get('type'),'host':urlparse(u).netloc})
   except Exception: pass
  body=d.find_element('tag name','body').text[:20000]
  (OUT/'network.json').write_text(json.dumps(events,ensure_ascii=False,indent=2),encoding='utf-8')
  (OUT/'body.txt').write_text(body,encoding='utf-8'); (OUT/'page.html').write_text(d.page_source,encoding='utf-8'); d.save_screenshot(str(OUT/'page.png'))
  hosts=sorted({e['host'] for e in events}); candidates=[e for e in events if any(k in e['url'].lower() for k in ['api','ajax','json','zco','broker','chip','buy','sell'])]
  report={'version':'A2_009_NETWORK_PROBE_V1.0.0','target':TARGET,'requests':len(events),'hosts':hosts,'candidate_count':len(candidates),'candidates':candidates[:100]}
  (OUT/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8'); print(json.dumps(report,ensure_ascii=False)); return 0
 finally: d.quit()
if __name__=='__main__': raise SystemExit(main())
