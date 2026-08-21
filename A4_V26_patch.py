from pathlib import Path
import runpy

cp = Path('A4_001A_CompleteControlPoint.py')
s = cp.read_text(encoding='utf-8')
if 'A4_001A_COMPLETE_CONTROL_POINT_V2.4.0' in s:
    runpy.run_path('A4_V25_patch.py', run_name='__main__')
    s = cp.read_text(encoding='utf-8')

assert 'A4_001A_COMPLETE_CONTROL_POINT_V2.5.0' in s, 'unexpected base version for V2.6'
s = s.replace('A4_001A_COMPLETE_CONTROL_POINT_V2.5.0', 'A4_001A_COMPLETE_CONTROL_POINT_V2.6.0', 1)
s = s.replace('QTS-A4-Complete-Control-Point/2.4', 'QTS-A4-Complete-Control-Point/2.6', 1)

# Sustainable full-market transport profile.
# 400 symbols/request was proven invalid by HTTP 414. 200 keeps the encoded URI
# below the observed server limit while reducing traffic ~3x vs the old profile.
s = s.replace('"POLL_SECONDS": str(POLL)', '"POLL_SECONDS": "30"', 1)
s = s.replace('"BATCH_SIZE": "100", "PRIMARY_WORKERS": "4", "RETRY_BATCH_SIZE": "40", "RETRY_WORKERS": "2", "SINGLE_FALLBACK_LIMIT": "5", "SESSION_POOL_SIZE": "4"', '"BATCH_SIZE": "200", "PRIMARY_WORKERS": "2", "RETRY_BATCH_SIZE": "50", "RETRY_WORKERS": "1", "SINGLE_FALLBACK_LIMIT": "2", "SESSION_POOL_SIZE": "2"', 1)
s = s.replace('"PRIMARY_TIMEOUT": "1.80", "RETRY_TIMEOUT": "1.50", "SINGLE_TIMEOUT": "1.20", "CYCLE_BUDGET_SECONDS": "9.0", "CYCLE_GUARD_SECONDS": "0.25"', '"PRIMARY_TIMEOUT": "4.00", "RETRY_TIMEOUT": "3.00", "SINGLE_TIMEOUT": "2.00", "CYCLE_BUDGET_SECONDS": "15.0", "CYCLE_GUARD_SECONDS": "0.40"', 1)
s = s.replace('core.PRIMARY_WORKERS = max(4, int(core.PRIMARY_WORKERS))', 'core.PRIMARY_WORKERS = max(2, int(core.PRIMARY_WORKERS))', 1)
s = s.replace('core.RETRY_WORKERS = max(2, int(core.RETRY_WORKERS))', 'core.RETRY_WORKERS = max(1, int(core.RETRY_WORKERS))', 1)
s = s.replace('core.SESSION_POOL_SIZE = max(4, int(core.SESSION_POOL_SIZE))', 'core.SESSION_POOL_SIZE = max(2, int(core.SESSION_POOL_SIZE))', 1)
s = s.replace('max_guard_records = max(6, int(math.ceil((RUNTIME_AUDIT_SECONDS * 2.5) / max(1.0, core.POLL_SECONDS))))', 'max_guard_records = max(6, int(math.ceil((RUNTIME_AUDIT_SECONDS * 4.0) / max(1.0, core.POLL_SECONDS))))', 1)

cp.write_text(s, encoding='utf-8')
print('A4 V2.6 sustainable transport patch applied: batch=200 poll=30s workers=2')
