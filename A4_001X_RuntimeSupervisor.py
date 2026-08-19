from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_001X_SUPERVISOR_V1.0.0"
ROOT = Path(__file__).resolve().parent
DEFAULT_SCRIPT = ROOT / "A4_001_TWSE_MIS盤前試撮抓取.py"
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "output/mis"))
START_TIME = dtime.fromisoformat(os.getenv("START_TIME", "08:30:00"))
END_TIME = dtime.fromisoformat(os.getenv("END_TIME", "09:30:00"))
RESTART_CUTOFF = dtime.fromisoformat(os.getenv("SUPERVISOR_RESTART_CUTOFF", "09:25:00"))
MAX_RESTARTS = min(5, max(0, int(os.getenv("SUPERVISOR_MAX_RESTARTS", "2"))))
HEARTBEAT_STALE_SECONDS = max(5.0, float(os.getenv("SUPERVISOR_HEARTBEAT_STALE_SECONDS", "18")))
START_GRACE_SECONDS = max(5.0, float(os.getenv("SUPERVISOR_START_GRACE_SECONDS", "25")))
CHECK_SECONDS = min(5.0, max(0.25, float(os.getenv("SUPERVISOR_CHECK_SECONDS", "1"))))
KILL_GRACE_SECONDS = min(10.0, max(1.0, float(os.getenv("SUPERVISOR_KILL_GRACE_SECONDS", "3"))))


def now_tw() -> datetime:
    return datetime.now(TZ)


def combine_today(t: dtime, base: datetime | None = None) -> datetime:
    base = base or now_tw()
    return datetime.combine(base.date(), t, TZ)


def atomic_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def append_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")
        f.flush()


def heartbeat_age(path: Path, now: datetime) -> tuple[float | None, str | None]:
    if not path.exists():
        return None, "MISSING"
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
        raw = str(obj.get("captured_at", "")).strip()
        if not raw:
            return None, "NO_CAPTURED_AT"
        captured = datetime.fromisoformat(raw)
        if captured.tzinfo is None:
            captured = captured.replace(tzinfo=TZ)
        return max(0.0, (now - captured.astimezone(TZ)).total_seconds()), None
    except Exception as exc:
        return None, f"BAD_HEARTBEAT:{type(exc).__name__}:{exc}"


def terminate_process(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is not None:
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
    except Exception:
        try:
            proc.terminate()
        except Exception:
            return
    deadline = time.monotonic() + KILL_GRACE_SECONDS
    while proc.poll() is None and time.monotonic() < deadline:
        time.sleep(0.1)
    if proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


def supervise(command: list[str], *, test_allow_early_success: bool = False) -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    base = now_tw()
    day = base.strftime("%Y-%m-%d")
    heartbeat = OUTPUT_DIR / f"heartbeat_{day}.json"
    journal = OUTPUT_DIR / f"supervisor_{day}.ndjson"
    latest = OUTPUT_DIR / f"supervisor_{day}.json"
    start_at = combine_today(START_TIME, base)
    end_at = combine_today(END_TIME, base)
    restart_cutoff = combine_today(RESTART_CUTOFF, base)

    restarts = 0
    launches = 0
    reasons: list[str] = []
    current: subprocess.Popen[Any] | None = None
    launched_at: datetime | None = None
    final_rc: int | None = None

    def launch(reason: str) -> None:
        nonlocal current, launches, launched_at
        launches += 1
        launched_at = now_tw()
        append_event(journal, {
            "version": VERSION,
            "at": launched_at.isoformat(),
            "event": "LAUNCH",
            "launch": launches,
            "restart_count": restarts,
            "reason": reason,
            "command": command,
        })
        current = subprocess.Popen(command, cwd=str(ROOT), start_new_session=True)

    launch("INITIAL")

    while True:
        assert current is not None
        now = now_tw()
        rc = current.poll()

        if rc is not None:
            final_rc = rc
            append_event(journal, {
                "version": VERSION,
                "at": now.isoformat(),
                "event": "PROCESS_EXIT",
                "returncode": rc,
                "restart_count": restarts,
            })
            early = now < end_at
            if rc == 0 and (not early or test_allow_early_success):
                break
            reason = "EARLY_EXIT" if rc == 0 else f"PROCESS_EXIT_{rc}"
            reasons.append(reason)
            if now >= restart_cutoff or restarts >= MAX_RESTARTS:
                break
            restarts += 1
            launch(reason)
            continue

        if now >= end_at + timedelta(seconds=30):
            reasons.append("PROCESS_OVERRAN_END_WINDOW")
            terminate_process(current)
            final_rc = current.poll()
            break

        check_heartbeat = now >= start_at + timedelta(seconds=START_GRACE_SECONDS)
        if check_heartbeat:
            age, hb_error = heartbeat_age(heartbeat, now)
            stale = hb_error is not None or age is None or age > HEARTBEAT_STALE_SECONDS
            if stale:
                reason = hb_error or f"HEARTBEAT_STALE_{age:.1f}s"
                append_event(journal, {
                    "version": VERSION,
                    "at": now.isoformat(),
                    "event": "HEARTBEAT_UNHEALTHY",
                    "reason": reason,
                    "age_seconds": age,
                    "restart_count": restarts,
                })
                reasons.append(reason)
                if now < restart_cutoff and restarts < MAX_RESTARTS:
                    terminate_process(current)
                    restarts += 1
                    launch(reason)
                    continue
                terminate_process(current)
                final_rc = current.poll()
                break

        time.sleep(CHECK_SECONDS)

    if current is not None and current.poll() is None:
        terminate_process(current)
        final_rc = current.poll()

    report = {
        "component": "A4_001X",
        "version": VERSION,
        "checked_at": now_tw().isoformat(),
        "command": command,
        "launches": launches,
        "restarts": restarts,
        "max_restarts": MAX_RESTARTS,
        "heartbeat_stale_seconds": HEARTBEAT_STALE_SECONDS,
        "final_returncode": final_rc,
        "recovery_reasons": reasons,
        "bounded_recovery": restarts <= MAX_RESTARTS,
        "pass": final_rc == 0,
    }
    atomic_json(latest, report)
    print(json.dumps(report, ensure_ascii=False))
    return report


def _write_test_worker(path: Path) -> None:
    path.write_text(
        """import json, os, sys, time\n"
        "from datetime import datetime\n"
        "from pathlib import Path\n"
        "from zoneinfo import ZoneInfo\n"
        "tz=ZoneInfo('Asia/Taipei')\n"
        "out=Path(os.environ['OUTPUT_DIR']); out.mkdir(parents=True, exist_ok=True)\n"
        "state=Path(os.environ['TEST_STATE']); n=int(state.read_text() if state.exists() else '0')+1; state.write_text(str(n))\n"
        "mode=os.environ['TEST_BEHAVIOR']\n"
        "hb=out/f\"heartbeat_{datetime.now(tz).strftime('%Y-%m-%d')}.json\"\n"
        "def beat(): hb.write_text(json.dumps({'captured_at':datetime.now(tz).isoformat()}))\n"
        "if mode=='crash_once' and n==1: sys.exit(7)\n"
        "if mode=='hang_once' and n==1: beat(); time.sleep(3); sys.exit(0)\n"
        "if mode=='always_fail': sys.exit(9)\n"
        "for _ in range(6): beat(); time.sleep(0.12)\n"
        "sys.exit(0)\n""",
        encoding="utf-8",
    )


def selftest() -> int:
    global OUTPUT_DIR, START_TIME, END_TIME, RESTART_CUTOFF, MAX_RESTARTS, HEARTBEAT_STALE_SECONDS, START_GRACE_SECONDS, CHECK_SECONDS, KILL_GRACE_SECONDS
    original = (OUTPUT_DIR, START_TIME, END_TIME, RESTART_CUTOFF, MAX_RESTARTS, HEARTBEAT_STALE_SECONDS, START_GRACE_SECONDS, CHECK_SECONDS, KILL_GRACE_SECONDS)
    results: dict[str, Any] = {}
    try:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            worker = root / "worker.py"
            _write_test_worker(worker)
            now = now_tw()
            START_TIME = (now - timedelta(seconds=1)).time().replace(microsecond=0)
            END_TIME = (now + timedelta(seconds=20)).time().replace(microsecond=0)
            RESTART_CUTOFF = (now + timedelta(seconds=15)).time().replace(microsecond=0)
            MAX_RESTARTS = 2
            HEARTBEAT_STALE_SECONDS = 0.7
            START_GRACE_SECONDS = 0.2
            CHECK_SECONDS = 0.1
            KILL_GRACE_SECONDS = 0.5

            for behavior in ("normal", "crash_once", "hang_once", "always_fail"):
                case = root / behavior
                case.mkdir()
                OUTPUT_DIR = case / "out"
                os.environ["OUTPUT_DIR"] = str(OUTPUT_DIR)
                os.environ["TEST_STATE"] = str(case / "state.txt")
                os.environ["TEST_BEHAVIOR"] = behavior
                report = supervise([sys.executable, str(worker)], test_allow_early_success=True)
                results[behavior] = report

        checks = {
            "normal_no_restart": results["normal"]["pass"] and results["normal"]["restarts"] == 0,
            "crash_recovered": results["crash_once"]["pass"] and results["crash_once"]["restarts"] == 1,
            "hang_recovered": results["hang_once"]["pass"] and results["hang_once"]["restarts"] == 1,
            "bounded_failure": (not results["always_fail"]["pass"]) and results["always_fail"]["restarts"] == 2,
        }
        report = {"component": "A4_001X", "version": VERSION, "mode": "selftest", "checks": checks, "cases": results, "pass": all(checks.values())}
        print(json.dumps(report, ensure_ascii=False))
        return 0 if report["pass"] else 2
    finally:
        OUTPUT_DIR, START_TIME, END_TIME, RESTART_CUTOFF, MAX_RESTARTS, HEARTBEAT_STALE_SECONDS, START_GRACE_SECONDS, CHECK_SECONDS, KILL_GRACE_SECONDS = original


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--script", default=str(DEFAULT_SCRIPT))
    args = p.parse_args()
    if args.self_test:
        return selftest()
    report = supervise([sys.executable, args.script])
    return 0 if report.get("pass") else 3


if __name__ == "__main__":
    raise SystemExit(main())
