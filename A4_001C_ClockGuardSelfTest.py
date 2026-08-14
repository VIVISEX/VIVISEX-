from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date, time
from pathlib import Path

WORKFLOW = Path('.github/workflows/scraper.yml')
TODAY = date(2026, 8, 17)


@dataclass(frozen=True)
class Trigger:
    date_value: date
    attempt: int
    requested_by: str


def authorized(now: time, trigger: Trigger, completed: bool = False) -> tuple[bool, str]:
    if completed:
        return False, 'already_completed'
    minute = now.hour * 60 + now.minute
    if minute < 8 * 60 + 10 or minute > 8 * 60 + 35:
        return False, 'outside_primary_recovery_window'
    if trigger.date_value != TODAY:
        return False, 'stale_trigger_date'
    if trigger.requested_by not in {'ChatGPT_A4_PrimaryClock', 'ChatGPT_A4_Watchdog'}:
        return False, 'unauthorized_trigger_source'
    if trigger.attempt not in {0, 1, 2}:
        return False, 'invalid_trigger_attempt'
    return True, 'authorized_live_start'


def final_gate_wait_seconds(now: time) -> int:
    now_sec = now.hour * 3600 + now.minute * 60 + now.second
    target = 8 * 3600 + 28 * 60 + 30
    return max(0, target - now_sec)


def main() -> int:
    cases = [
        ('before_window', time(8, 9), Trigger(TODAY, 0, 'ChatGPT_A4_PrimaryClock'), False, False),
        ('primary_0815', time(8, 15), Trigger(TODAY, 0, 'ChatGPT_A4_PrimaryClock'), False, True),
        ('watchdog_0827', time(8, 27), Trigger(TODAY, 1, 'ChatGPT_A4_Watchdog'), False, True),
        ('watchdog_0829', time(8, 29), Trigger(TODAY, 2, 'ChatGPT_A4_Watchdog'), False, True),
        ('after_window', time(8, 36), Trigger(TODAY, 2, 'ChatGPT_A4_Watchdog'), False, False),
        ('stale_date', time(8, 20), Trigger(date(2026, 8, 14), 0, 'ChatGPT_A4_PrimaryClock'), False, False),
        ('unauthorized_source', time(8, 20), Trigger(TODAY, 0, 'UNKNOWN'), False, False),
        ('attempt_overflow', time(8, 20), Trigger(TODAY, 3, 'ChatGPT_A4_Watchdog'), False, False),
        ('already_completed', time(8, 20), Trigger(TODAY, 0, 'ChatGPT_A4_PrimaryClock'), True, False),
    ]

    results = []
    for name, now, trigger, completed, expected in cases:
        decision, reason = authorized(now, trigger, completed)
        results.append({
            'name': name,
            'expected': expected,
            'actual': decision,
            'reason': reason,
            'pass': decision == expected,
        })

    wait_cases = [
        ('primary_runner_0815', time(8, 15, 0), 810),
        ('recovery_0827', time(8, 27, 0), 90),
        ('one_second_before_gate', time(8, 28, 29), 1),
        ('exact_gate_time', time(8, 28, 30), 0),
        ('late_recovery_0829', time(8, 29, 0), 0),
    ]
    wait_results = []
    for name, now, expected in wait_cases:
        actual = final_gate_wait_seconds(now)
        wait_results.append({
            'name': name,
            'expected_wait_seconds': expected,
            'actual_wait_seconds': actual,
            'pass': actual == expected,
        })

    text = WORKFLOW.read_text(encoding='utf-8')
    workflow_checks = {
        'push_start_0810': 'PUSH_START=$((8 * 60 + 10))' in text,
        'push_end_0835': 'PUSH_END=$((8 * 60 + 35))' in text,
        'primary_clock_allowed': 'ChatGPT_A4_PrimaryClock|ChatGPT_A4_Watchdog' in text,
        'attempt_range_enforced': bool(re.search(r'TRIGGER_ATTEMPT.*\^\[0-2\]\$', text, re.S)),
        'stale_date_guard': 'reason=stale_trigger_date' in text,
        'completed_guard': 'reason=already_completed' in text,
        'push_cancels_running': "cancel-in-progress: ${{ github.event_name == 'push'" in text,
        'schedule_backup_present': 'cron: "25 0 * * 1-5"' in text,
        'final_market_gate_target_082830': 'TARGET_SEC=$((8 * 3600 + 28 * 60 + 30))' in text,
        'final_market_gate_live_only': "steps.guard.outputs.should_run == 'true' && env.RUN_MODE == 'live'" in text,
        'final_market_gate_before_scraper': text.find('Wait until final market-gate window') < text.find('TWSE trading-day and emergency-closure gate') < text.find('Run A4 MIS scraper'),
        'status_push_retry_present': 'A4 status push failed after 5 attempts' in text,
        'executed_sha_recorded': 'EXECUTED_SHA=$(git rev-parse HEAD)' in text,
    }

    passed = (
        all(item['pass'] for item in results)
        and all(item['pass'] for item in wait_results)
        and all(workflow_checks.values())
    )
    report = {
        'component': 'A4_001C',
        'purpose': 'Deterministic primary-clock, recovery guard, and final market-gate timing validation',
        'cases': results,
        'final_gate_wait_cases': wait_results,
        'workflow_checks': workflow_checks,
        'pass': passed,
    }
    out = Path('output/clock_readiness')
    out.mkdir(parents=True, exist_ok=True)
    (out / 'clock_guard_selftest.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(report, ensure_ascii=False))
    return 0 if passed else 1


if __name__ == '__main__':
    raise SystemExit(main())
