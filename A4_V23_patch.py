from pathlib import Path
import re
import subprocess

ENGINE = Path('A4_001_TWSE_MIS盤前試撮抓取.py')
WORKFLOW = Path('.github/workflows/scraper.yml')
LEGACY_PATCH = Path('A4_V24_patch.py')


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if old not in text:
        raise RuntimeError(f'PATCH_ANCHOR_MISSING:{label}')
    return text.replace(old, new, 1)


e = ENGINE.read_text(encoding='utf-8')
if 'VERSION = "1.5.0"' in e:
    e = replace_once(e, 'VERSION = "1.5.0"', 'VERSION = "1.6.0"', 'engine_version')
    e = e.replace('QTS-A4/1.5', 'QTS-A4/1.6')
    pattern = re.compile(r'def warm_session\(session: requests\.Session\) -> dict\[str, Any\]:\n.*?\n\ndef init_session_pool\(\)', re.S)
    new_warm = '''def warm_session(session: requests.Session) -> dict[str, Any]:\n    errors: list[str] = []\n    probes = {"2330", "6488"}\n    for attempt in range(1, WARMUP_ATTEMPTS + 1):\n        try:\n            r = session.get(MIS_URL, params={"ex_ch": "tse_2330.tw|otc_6488.tw", "json": "1", "delay": "0", "_": str(int(time.time() * 1000))}, timeout=WARMUP_TIMEOUT)\n            r.raise_for_status()\n            obj = r.json()\n            items = obj.get("msgArray", []) if isinstance(obj, dict) else []\n            codes = {str(x.get("c", "")).strip() for x in items if isinstance(x, dict)}\n            if str(obj.get("rtcode", "")) == "0000" and probes.issubset(codes):\n                return {"ok": True, "attempt": attempt, "status": r.status_code, "url": MIS_URL, "cookie_count": len(session.cookies), "probe_symbols": sorted(probes), "returned_probe_symbols": sorted(codes & probes)}\n            errors.append(f"MIS_API_SCHEMA rtcode={obj.get('rtcode') if isinstance(obj, dict) else None} codes={sorted(codes & probes)}")\n        except Exception as exc:\n            et, _ = classify_exception(exc)\n            errors.append(f"MIS_API:{et}:{exc}")\n        if attempt < WARMUP_ATTEMPTS:\n            time.sleep(min(BACKOFF_CAP_SECONDS, BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))))\n    return {"ok": False, "errors": errors[-6:], "cookie_count": len(session.cookies), "probe_symbols": sorted(probes)}\n\n\ndef init_session_pool()'''
    e, n = pattern.subn(new_warm, e, count=1)
    if n != 1:
        raise RuntimeError('PATCH_ANCHOR_MISSING:engine_warm_session')
    heartbeat_anchor = '                    "data_health": "SOURCE_GAP" if source_gap_active else "NORMAL",\n'
    e = replace_once(e, heartbeat_anchor, heartbeat_anchor + '                    "runtime_guard": diag.get("runtime_guard"),\n', 'engine_heartbeat_guard')
    ENGINE.write_text(e, encoding='utf-8')

w = WORKFLOW.read_text(encoding='utf-8')
if 'RUNTIME_AUDIT_SECONDS:' not in w:
    anchor = '      MAX_ALLOWED_GAP_SECONDS: "30"\n'
    insert = anchor + '''      RUNTIME_AUDIT_SECONDS: "30"\n      RUNTIME_MIN_COVERAGE: "0.97"\n      RUNTIME_MAX_ERROR_RATE: "0.05"\n      RUNTIME_MAX_REPAIRS: "3"\n      RUNTIME_FAIL_CLOSED_WINDOWS: "3"\n'''
    w = replace_once(w, anchor, insert, 'workflow_runtime_guard_env')
    WORKFLOW.write_text(w, encoding='utf-8')

if LEGACY_PATCH.exists():
    LEGACY_PATCH.unlink()

subprocess.run(['git', 'add', str(ENGINE), str(WORKFLOW)], check=True)
subprocess.run(['git', 'add', '-A', str(LEGACY_PATCH)], check=True)
print('A4 V2.4 engine/workflow alignment staged')
