# trigger 2026-08-21T09:04:40+08:00
from pathlib import Path

cp = Path('A4_001A_CompleteControlPoint.py')
s = cp.read_text(encoding='utf-8')
assert 'A4_001A_COMPLETE_CONTROL_POINT_V2.4.0' in s, 'unexpected control version'
s = s.replace('A4_001A_COMPLETE_CONTROL_POINT_V2.4.0', 'A4_001A_COMPLETE_CONTROL_POINT_V2.5.0', 1)
s = s.replace('if str(obj.get("rtcode", "")) == "0000" and probes.issubset(codes):', 'if str(obj.get("rtcode", "")) == "0000" and bool(codes & probes):', 1)
needle = '        core.warm_session = validated_warm_session\n\n'
insert = '''        core.warm_session = validated_warm_session\n\n        # Transport errors on one session must not freeze every healthy session.\n        _original_register_backoff = core.register_backoff\n        def qts_register_backoff(error_type: str | None) -> None:\n            if error_type in {"HTTP_408", "HTTP_425", "HTTP_429", "HTTP_500", "HTTP_502", "HTTP_503", "HTTP_504"}:\n                _original_register_backoff(error_type)\n        core.register_backoff = qts_register_backoff\n\n'''
assert needle in s
s = s.replace(needle, insert, 1)
s = s.replace('guard_state = {"repairs": 0, "bad_windows": 0, "last_audit_mono": time.monotonic() - RUNTIME_AUDIT_SECONDS, "profile": "NORMAL"}', 'guard_state = {"repairs": 0, "bad_windows": 0, "consecutive_bad_cycles": 0, "healthy_windows": 0, "last_audit_mono": time.monotonic() - RUNTIME_AUDIT_SECONDS, "profile": "NORMAL"}', 1)
start = s.index('        def write_guard(event: dict[str, Any]) -> None:')
end_marker = '        core.capture_cycle = guarded_capture'
end = s.index(end_marker, start) + len(end_marker)
new_block = r'''        def publish_guard_status(event: dict[str, Any]) -> None:
            token = os.getenv("GITHUB_TOKEN", "").strip()
            repo = os.getenv("GITHUB_REPOSITORY", "").strip()
            sha = os.getenv("GITHUB_SHA", "").strip()
            if not token or not repo or not sha:
                return
            status = str(event.get("status", ""))
            state = "success" if status in {"PASS_WARMING", "STABLE"} else "failure" if status == "FAIL_CLOSED" else "pending"
            desc = (
                f"{status} cov={float(event.get('average_coverage', 0.0))*100:.2f}% "
                f"err={float(event.get('request_error_rate', 0.0))*100:.2f}% "
                f"stable={int(event.get('healthy_windows', 0))} repairs={int(event.get('repairs', 0))}"
            )[:140]
            try:
                requests.post(
                    f"https://api.github.com/repos/{repo}/statuses/{sha}",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/vnd.github+json",
                        "X-GitHub-Api-Version": "2022-11-28",
                        "User-Agent": UA,
                    },
                    json={"state": state, "context": "qts/a4/runtime-guard", "description": desc},
                    timeout=8,
                ).raise_for_status()
            except Exception:
                pass

        def write_guard(event: dict[str, Any]) -> None:
            event = {"component": COMPONENT, "version": VERSION, "engine_version": getattr(core, "VERSION", "unknown"), "checked_at": iso(), **event}
            atomic_json(guard_latest, event)
            with guard_log.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(event, ensure_ascii=False) + "\n")
            publish_guard_status(event)

        def repair_runtime(reason: str) -> dict[str, Any]:
            guard_state["repairs"] += 1
            attempt = int(guard_state["repairs"])
            core.PRIMARY_WORKERS = max(4, int(core.PRIMARY_WORKERS))
            core.RETRY_WORKERS = max(2, int(core.RETRY_WORKERS))
            core.SESSION_POOL_SIZE = max(4, int(core.SESSION_POOL_SIZE))
            core.RETRY_TRIGGER_COVERAGE = max(float(core.RETRY_TRIGGER_COVERAGE), 0.92)
            core.SINGLE_FALLBACK_LIMIT = min(int(core.SINGLE_FALLBACK_LIMIT), 4)
            core._BACKOFF_UNTIL = 0.0
            core._BACKOFF_LEVEL = 0
            guard_state["profile"] = f"POOL_RECYCLE_{attempt}"
            warm = core.init_session_pool()
            guard_state["consecutive_bad_cycles"] = 0
            return {
                "attempt": attempt, "reason": reason, "profile": guard_state["profile"],
                "primary_workers": core.PRIMARY_WORKERS, "retry_workers": core.RETRY_WORKERS,
                "session_pool_size": core.SESSION_POOL_SIZE, "retry_trigger_coverage": core.RETRY_TRIGGER_COVERAGE,
                "warmup_ok_count": sum(1 for x in warm if x.get("ok")), "warmup_total": len(warm),
            }

        def guarded_capture(stocks: list[Any], cycle_deadline: float):
            cycle_started = time.monotonic()
            results, diag = original_capture(stocks, cycle_deadline)
            merged = results[0]
            strict_date = core.RUN_MODE == "live"
            today_compact = core.now_tw().strftime("%Y%m%d")
            accepted: list[dict[str, Any]] = []
            seen_codes: set[str] = set()
            integrity = Counter()
            for item in list(merged.items):
                if not isinstance(item, dict): integrity["non_object"] += 1; continue
                code = str(item.get("c", "")).strip(); market_date = str(item.get("d", "")).strip(); market = str(item.get("ex", "")).strip().lower()
                if not code or not market_date or not market: integrity["schema_missing"] += 1; continue
                if code not in expected_codes: integrity["unexpected_code"] += 1; continue
                expected_ex = "tse" if expected_market[code] == "TSE" else "otc"
                if market != expected_ex: integrity["market_mismatch"] += 1; continue
                if strict_date and market_date != today_compact: integrity["stale_date"] += 1; continue
                if code in seen_codes: integrity["duplicate_code"] += 1; continue
                seen_codes.add(code); accepted.append(item)
            merged.items = accepted
            accepted_codes = {str(x.get("c", "")).strip() for x in accepted}
            coverage = len(accepted_codes) / len(expected_codes) if expected_codes else 0.0
            attempts = int(diag.get("request_attempts") or 0); errors = int(diag.get("request_errors") or 0)
            error_rate = errors / attempts if attempts else 1.0; elapsed = time.monotonic() - cycle_started
            required_coverage = max(RUNTIME_MIN_COVERAGE, min(1.0, PER_CYCLE))
            cycle_ok = coverage >= required_coverage and error_rate <= RUNTIME_MAX_ERROR_RATE and not any(integrity.values())
            guard_state["consecutive_bad_cycles"] = 0 if cycle_ok else int(guard_state["consecutive_bad_cycles"]) + 1
            record = {"mono": time.monotonic(), "coverage": coverage, "required_coverage": required_coverage, "attempts": attempts, "errors": errors, "error_rate": error_rate, "elapsed_seconds": elapsed, "integrity": dict(integrity), "accepted_symbols": len(accepted_codes), "requested_symbols": len(expected_codes), "missing_symbols": sorted(expected_codes - accepted_codes)[:100], "cycle_ok": cycle_ok, "consecutive_bad_cycles": int(guard_state["consecutive_bad_cycles"])}
            guard_records.append(record)
            diag["missing_symbols"] = sorted(expected_codes - accepted_codes); diag["returned_symbols"] = len(accepted_codes); diag["runtime_guard"] = record
            now_mono = time.monotonic(); audit_due = core.RUN_MODE == "env_test" or (now_mono - float(guard_state["last_audit_mono"]) >= RUNTIME_AUDIT_SECONDS)
            if audit_due:
                guard_state["last_audit_mono"] = now_mono
                floor = now_mono - RUNTIME_AUDIT_SECONDS - max(1.0, core.POLL_SECONDS); window = [x for x in guard_records if float(x["mono"]) >= floor]
                total_attempts = sum(int(x["attempts"]) for x in window); total_errors = sum(int(x["errors"]) for x in window); window_error_rate = total_errors / total_attempts if total_attempts else 1.0
                coverages = sorted(float(x["coverage"]) for x in window); avg_coverage = sum(coverages) / len(coverages) if coverages else 0.0; median_coverage = coverages[len(coverages)//2] if coverages else 0.0; min_coverage = min(coverages, default=0.0)
                integrity_totals = Counter(); [integrity_totals.update(x.get("integrity", {})) for x in window]
                good_cycles = sum(1 for x in window if x.get("cycle_ok")); bad_cycles = len(window) - good_cycles; good_ratio = good_cycles / len(window) if window else 0.0
                window_ok = bool(window) and bool(record["cycle_ok"]) and good_ratio >= 0.80 and median_coverage >= required_coverage and window_error_rate <= max(0.10, RUNTIME_MAX_ERROR_RATE * 2) and not any(integrity_totals.values())
                common = {"profile": guard_state["profile"], "repairs": guard_state["repairs"], "window_cycles": len(window), "good_ratio": good_ratio, "bad_cycles": bad_cycles, "average_coverage": avg_coverage, "median_coverage": median_coverage, "minimum_coverage": min_coverage, "request_error_rate": window_error_rate, "integrity": dict(integrity_totals)}
                if window_ok:
                    guard_state["bad_windows"] = 0; guard_state["healthy_windows"] += 1
                    write_guard({"status": "STABLE" if int(guard_state["healthy_windows"]) >= 3 else "PASS_WARMING", "healthy_windows": guard_state["healthy_windows"], **common})
                elif int(guard_state["consecutive_bad_cycles"]) < 2:
                    guard_state["healthy_windows"] = 0; write_guard({"status": "DEGRADED_OBSERVE", "healthy_windows": 0, "consecutive_bad_cycles": guard_state["consecutive_bad_cycles"], **common})
                else:
                    guard_state["healthy_windows"] = 0; guard_state["bad_windows"] += 1
                    reason = "DATA_INTEGRITY" if any(integrity_totals.values()) else "TRANSPORT_ERROR" if window_error_rate > RUNTIME_MAX_ERROR_RATE else "LOW_COVERAGE" if median_coverage < required_coverage else "SOURCE_GAP"
                    recovery = repair_runtime(reason) if int(guard_state["repairs"]) < RUNTIME_MAX_REPAIRS else None
                    status = "AUTO_REPAIRED" if recovery else "FAIL_CLOSED" if int(guard_state["bad_windows"]) >= RUNTIME_FAIL_CLOSED_WINDOWS else "DEGRADED_OBSERVE"
                    write_guard({"status": status, "reason": reason, "healthy_windows": 0, "bad_windows": guard_state["bad_windows"], "recovery": recovery, **common})
                    if status == "FAIL_CLOSED": raise RuntimeError(f"RUNTIME_DATA_GUARD_FAIL_CLOSED reason={reason} bad_windows={guard_state['bad_windows']} repairs={guard_state['repairs']}")
            return results, diag

        core.capture_cycle = guarded_capture'''
s = s[:start] + new_block + s[end:]
old = 'passed=bool(engine_clean_exit and audit["pass"] and clean["pass"])\n    failure_reasons=[k.upper() for k,v in audit["gates"].items() if not v]+([] if clean["pass"] else ["CLEAN_FAIL"])+([] if engine_clean_exit else ["ENGINE_NONZERO_EXIT"])\n    runtime_guard=read_json(OUT/"engine"/f"runtime_guard_{day}.json") or {}'
new = 'runtime_guard=read_json(OUT/"engine"/f"runtime_guard_{day}.json") or {}\n    runtime_stable = runtime_guard.get("status") == "STABLE" and int(runtime_guard.get("healthy_windows", 0)) >= 3\n    passed=bool(engine_clean_exit and audit["pass"] and clean["pass"] and runtime_stable)\n    failure_reasons=[k.upper() for k,v in audit["gates"].items() if not v]+([] if clean["pass"] else ["CLEAN_FAIL"])+([] if engine_clean_exit else ["ENGINE_NONZERO_EXIT"])+([] if runtime_stable else ["RUNTIME_GUARD_NOT_STABLE"])'
assert old in s, 'formal acceptance block not found'; s = s.replace(old, new, 1); cp.write_text(s, encoding='utf-8')
wf = Path('.github/workflows/scraper.yml'); w = wf.read_text(encoding='utf-8'); needle = '      DEFAULT_BRANCH: ${{ github.event.repository.default_branch }}\n\n    steps:'; repl = '      DEFAULT_BRANCH: ${{ github.event.repository.default_branch }}\n      GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}\n\n    steps:'; assert needle in w; w = w.replace(needle, repl, 1); wf.write_text(w, encoding='utf-8')
print('A4 V2.5 patch staged successfully')
