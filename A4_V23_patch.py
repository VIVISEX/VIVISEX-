from pathlib import Path

P = Path('A4_001A_CompleteControlPoint.py')
s = P.read_text(encoding='utf-8')
if 'A4_001A_COMPLETE_CONTROL_POINT_V2.3.0' in s:
    print('A4 V2.3 already applied')
    raise SystemExit(0)
if 'A4_001A_COMPLETE_CONTROL_POINT_V2.1.0' not in s:
    raise SystemExit('PATCH_BASE_VERSION_MISMATCH')

s = s.replace('import argparse\n', 'import argparse\nimport contextlib\nimport importlib.util\nimport io\n', 1)
s = s.replace('VERSION = "A4_001A_COMPLETE_CONTROL_POINT_V2.1.0"', 'VERSION = "A4_001A_COMPLETE_CONTROL_POINT_V2.3.0"')
s = s.replace('UA = "Mozilla/5.0 QTS-A4-Complete-Control-Point/2.1"', 'UA = "Mozilla/5.0 QTS-A4-Complete-Control-Point/2.3"')
s = s.replace(
    'MAX_GAP = float(os.getenv("MAX_ALLOWED_GAP_SECONDS", "30"))\n',
    'MAX_GAP = float(os.getenv("MAX_ALLOWED_GAP_SECONDS", "30"))\n'
    'ENV_TEST_MIN_COVERAGE = float(os.getenv("ENV_TEST_MIN_COVERAGE", "0.95"))\n'
    'ENV_TEST_MAX_ERROR_RATE = float(os.getenv("ENV_TEST_MAX_ERROR_RATE", "0.05"))\n',
    1,
)

start = s.index('def run_engine(universe_path: Path, mode: str) -> tuple[int, str]:')
end = s.index('\n\ndef percentile(', start)
new_run_engine = '''def run_engine(universe_path: Path, mode: str) -> tuple[int, str]:
    """Run the MIS engine with API-level session warmup controlled here.

    The old HTML warmup pages now return HTTP 404 while the production MIS API
    remains healthy. Session admission therefore probes the same getStockInfo
    API used by production and requires both TSE and OTC representative symbols.
    """
    env = engine_env(universe_path)
    env["RUN_MODE"] = mode
    if mode == "env_test":
        env["MIN_SYMBOL_COVERAGE"] = str(ENV_TEST_MIN_COVERAGE)

    managed = set(env)
    previous = {k: os.environ.get(k) for k in managed}
    os.environ.update(env)
    module_name = f"qts_a4_engine_{os.getpid()}_{time.time_ns()}"
    buf = io.StringIO()
    try:
        spec = importlib.util.spec_from_file_location(module_name, ENGINE)
        if spec is None or spec.loader is None:
            raise RuntimeError("ENGINE_IMPORT_SPEC_FAIL")
        core = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = core
        spec.loader.exec_module(core)

        def validated_warm_session(session: requests.Session) -> dict[str, Any]:
            errors: list[str] = []
            probes = {"2330", "6488"}
            for attempt in range(1, core.WARMUP_ATTEMPTS + 1):
                try:
                    response = session.get(
                        core.MIS_URL,
                        params={
                            "ex_ch": "tse_2330.tw|otc_6488.tw",
                            "json": "1",
                            "delay": "0",
                            "_": str(int(time.time() * 1000)),
                        },
                        timeout=core.WARMUP_TIMEOUT,
                    )
                    response.raise_for_status()
                    obj = response.json()
                    items = obj.get("msgArray", []) if isinstance(obj, dict) else []
                    codes = {str(x.get("c", "")).strip() for x in items if isinstance(x, dict)}
                    if str(obj.get("rtcode", "")) == "0000" and probes.issubset(codes):
                        return {
                            "ok": True,
                            "attempt": attempt,
                            "status": response.status_code,
                            "url": core.MIS_URL,
                            "cookie_count": len(session.cookies),
                            "probe_symbols": sorted(probes),
                            "returned_probe_symbols": sorted(codes & probes),
                        }
                    errors.append(
                        f"MIS_API_SCHEMA rtcode={obj.get('rtcode') if isinstance(obj, dict) else None} "
                        f"codes={sorted(codes & probes)}"
                    )
                except Exception as exc:
                    et, _ = core.classify_exception(exc)
                    errors.append(f"MIS_API:{et}:{exc}")
                if attempt < core.WARMUP_ATTEMPTS:
                    time.sleep(min(core.BACKOFF_CAP_SECONDS, core.BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))))
            return {
                "ok": False,
                "errors": errors[-6:],
                "cookie_count": len(session.cookies),
                "probe_symbols": sorted(probes),
            }

        core.warm_session = validated_warm_session
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            rc = int(core.main())
        return rc, buf.getvalue()[-12000:]
    except Exception as exc:
        buf.write(f"\nCONTROL_ENGINE_FATAL:{type(exc).__name__}:{exc}\n")
        return 99, buf.getvalue()[-12000:]
    finally:
        sys.modules.pop(module_name, None)
        for key, old in previous.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old
'''
s = s[:start] + new_run_engine + s[end:]

start = s.index('def env_test() -> int:')
end = s.index('\n\ndef live()', start)
new_env = '''def env_test() -> int:
    pf = preflight("env_test")
    stocks, urep = universe()
    up = write_universe(stocks, urep, now().date().isoformat())
    gate_probe = market_gate(now().date())
    rc, log = run_engine(up, "env_test")
    eng = read_json(OUT / "engine" / "env_test_report.json") or {}

    requested = int(eng.get("requested_symbols") or 0)
    returned = int(eng.get("returned_symbols") or 0)
    attempts = int(eng.get("request_attempts") or 0)
    errors = int(eng.get("request_errors") or 0)
    coverage = (returned / requested) if requested else 0.0
    error_rate = (errors / attempts) if attempts else 1.0
    elapsed = float(eng.get("cycle_elapsed_seconds") or 999999.0)
    budget = float(eng.get("cycle_budget_seconds") or 0.0)
    warmup_ok = int(eng.get("warmup_ok_count") or 0)
    warmup_total = int(eng.get("warmup_total") or 0)

    gates = {
        "preflight": pf["status"] == "PASS",
        "universe_ready": urep["status"] == "READY",
        "market_gate_source_verified": bool(gate_probe.get("verified")),
        "universe_matches_engine_request": requested == len(stocks),
        "full_market_coverage": coverage >= ENV_TEST_MIN_COVERAGE,
        "request_error_rate": error_rate <= ENV_TEST_MAX_ERROR_RATE,
        "cycle_budget": budget > 0 and elapsed <= budget + 0.25,
        "warmup_available": warmup_total > 0 and warmup_ok > 0,
        "engine_process_clean_exit": rc == 0,
    }
    rep = {
        "component": COMPONENT,
        "version": VERSION,
        "mode": "env_test",
        "pass": all(gates.values()),
        "gates": gates,
        "preflight": pf,
        "universe": urep,
        "market_gate_probe": gate_probe,
        "requested_symbols": requested,
        "returned_symbols": returned,
        "coverage": coverage,
        "required_coverage": ENV_TEST_MIN_COVERAGE,
        "request_attempts": attempts,
        "request_errors": errors,
        "request_error_rate": error_rate,
        "allowed_request_error_rate": ENV_TEST_MAX_ERROR_RATE,
        "cycle_elapsed_seconds": elapsed,
        "cycle_budget_seconds": budget,
        "warmup_ok_count": warmup_ok,
        "warmup_total": warmup_total,
        "engine_returncode": rc,
        "engine": eng,
        "engine_log_tail": log[-3000:],
        "checked_at": iso(),
    }
    atomic_json(OUT / "env_test_report.json", rep)
    print(json.dumps(rep, ensure_ascii=False))
    return 0 if rep["pass"] else 3
'''
s = s[:start] + new_env + s[end:]

old = '''    passed=bool(audit["pass"] and clean["pass"])
    rep={"date":day,"mode":"live","pass":passed,"production_acceptance_applicable":True,"preflight":pf,"universe":urep,"market_gate":gate,"engine_returncode":rc,"engine_log_tail":log[-4000:],"audit":audit,"clean":clean,"checkpoint":str(CHECKPOINT),"legacy_fixed_stocks_csv_used":False,"failure_reasons":[k.upper() for k,v in audit["gates"].items() if not v]+([] if clean["pass"] else ["CLEAN_FAIL"])}
'''
new = '''    engine_clean_exit = rc == 0
    passed=bool(engine_clean_exit and audit["pass"] and clean["pass"])
    failure_reasons=[k.upper() for k,v in audit["gates"].items() if not v]+([] if clean["pass"] else ["CLEAN_FAIL"])+([] if engine_clean_exit else ["ENGINE_NONZERO_EXIT"])
    rep={"date":day,"mode":"live","pass":passed,"production_acceptance_applicable":True,"preflight":pf,"universe":urep,"market_gate":gate,"engine_returncode":rc,"engine_clean_exit":engine_clean_exit,"engine_log_tail":log[-4000:],"audit":audit,"clean":clean,"checkpoint":str(CHECKPOINT),"legacy_fixed_stocks_csv_used":False,"failure_reasons":failure_reasons}
'''
if old not in s:
    raise SystemExit('LIVE_GATE_PATCH_TARGET_NOT_FOUND')
s = s.replace(old, new, 1)

P.write_text(s, encoding='utf-8')
print('patched', P, 'bytes', len(s.encode('utf-8')))
