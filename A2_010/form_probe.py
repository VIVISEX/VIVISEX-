from __future__ import annotations
import json, re, time, urllib.request
from pathlib import Path
from urllib.parse import urljoin
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'output'; OUT.mkdir(parents=True,exist_ok=True)
HOST='https://5850web.moneydj.com'
URLS=[
 f'{HOST}/z/zc/zco/zco_2330.djhtm',
 f'{HOST}/z/zc/zco/zco0/zco0.djhtm?a=2330&BHID=1470&b=1470&C=1',
 f'{HOST}/z/zc/zco/zco0/zco0.djhtm?a=2330&b=1470&C=1',
]

def drv():
 o=Options(); o.add_argument('--headless=new'); o.add_argument('--no-sandbox'); o.add_argument('--disable-dev-shm-usage'); o.add_argument('--window-size=1920,1080'); o.add_argument('--lang=zh-TW'); return webdriver.Chrome(options=o)

def attrs(el,names):
 return {n:el.get_attribute(n) for n in names}

def main():
 d=drv(); report={'version':'A2_010_FORM_PROBE_V1.0.0','pages':[],'scripts':[]}
 try:
  for url in URLS:
   d.get(url); time.sleep(3)
   page={'requested':url,'current_url':d.current_url,'title':d.title,'forms':[],'body_head':d.find_element(By.TAG_NAME,'body').text[:6000]}
   for f in d.find_elements(By.TAG_NAME,'form'):
    fo=attrs(f,['name','id','method','action','onsubmit'])
    fo['inputs']=[]
    for e in f.find_elements(By.CSS_SELECTOR,'input,select,button'):
     item=attrs(e,['tagName','type','name','id','value','onclick','onchange'])
     if e.tag_name=='select':
      item['options']=[{'value':o.get_attribute('value'),'text':o.text,'selected':o.is_selected()} for o in e.find_elements(By.TAG_NAME,'option')][:80]
     fo['inputs'].append(item)
    page['forms'].append(fo)
   page['links']=[{'text':a.text,'href':a.get_attribute('href')} for a in d.find_elements(By.TAG_NAME,'a') if 'zco0' in (a.get_attribute('href') or '')][:30]
   scripts=[]
   for s in d.find_elements(By.TAG_NAME,'script'):
    src=s.get_attribute('src') or ''
    if src and ('broker' in src.lower() or 'zco' in src.lower()): scripts.append(src)
   page['script_srcs']=scripts
   report['pages'].append(page)
  # Fetch discovered broker scripts directly, preserving source for exact parameter logic.
  script_urls=[]
  for p in report['pages']:
   for s in p.get('script_srcs',[]):
    if s not in script_urls: script_urls.append(s)
  for i,u in enumerate(script_urls):
   try:
    req=urllib.request.Request(u,headers={'User-Agent':'Mozilla/5.0'})
    raw=urllib.request.urlopen(req,timeout=30).read()
    text=raw.decode('big5','replace')
    fn=f'script_{i}.js'; (OUT/fn).write_text(text,encoding='utf-8')
    report['scripts'].append({'url':u,'file':fn,'chars':len(text),'tokens':sorted(set(re.findall(r'\b(?:BHID|[ABCDE]|from|to|year|month|day|date)\b',text,re.I)))})
   except Exception as e:
    report['scripts'].append({'url':u,'error':repr(e)})
  (OUT/'report.json').write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')
  print(json.dumps({'pages':len(report['pages']),'scripts':report['scripts']},ensure_ascii=False))
 finally:
  d.quit()
if __name__=='__main__': raise SystemExit(main())
