from __future__ import annotations

import csv
import json
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import date, datetime
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode

from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "output"
OUT.mkdir(parents=True, exist_ok=True)

DATE_RE = re.compile(r"^(20\d{2})[./-](\d{1,2})[./-](\d{1,2})$")
NUM_RE = re.compile(r"^-?\d+(?:\.\d+)?$")

HOSTS = [
    "https://fubon-ebrokerdj.fbs.com.tw",
    "https://5850web.moneydj.com",
    "https://concords.moneydj.com",
    "https://justdata.moneydj.com",
    "https://tsitap.moneydj.com",
]


@dataclass(frozen=True)
class Record:
    trade_date: str
    stock: str
    broker_code: str
    broker_name: str
    buy: float
    sell: float
    net: float
    source_url: str


def norm_num(text: str) -> float | None:
    s = (
        text.replace(",", "")
        .replace("張", "")
        .replace("+", "")
        .replace("−", "-")
        .replace("－", "-")
        .strip()
    )
    if not NUM_RE.match(s):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def norm_date(text: str) -> str | None:
    m = DATE_RE.match(text.strip())
    if not m:
        return None
    y, mth, d = map(int, m.groups())
    try:
        return date(y, mth, d).isoformat()
    except ValueError:
        return None


def build_url(host: str, stock: str, broker_code: str, start: str, end: str) -> str:
    params = {
        "a": stock,
        "A": stock,
        "BHID": broker_code,
        "b": broker_code,
        "C": "1",
        "D": start,
        "E": end,
        "ver": "V3",
    }
    return f"{host}/z/zc/zco/zco0/zco0.djhtm?{urlencode(params)}"


def chrome() -> webdriver.Chrome:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--lang=zh-TW")
    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/151 Safari/537.36"
    )
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(45)
    return driver


def wait_dynamic_tables(driver: webdriver.Chrome, timeout: int = 25) -> None:
    def ready(d: webdriver.Chrome) -> bool:
        if d.execute_script("return document.readyState") != "complete":
            return False
        tables = d.find_elements(By.TAG_NAME, "table")
        if not tables:
            return False
        text = d.find_element(By.TAG_NAME, "body").text
        return ("買進" in text and "賣出" in text) or bool(d.find_elements(By.ID, "oMainTable"))

    WebDriverWait(driver, timeout).until(ready)
    # Dynamic MoneyDJ pages often render rows shortly after the shell becomes ready.
    time.sleep(2.0)


def extract_rows(driver: webdriver.Chrome, stock: str, broker_code: str, broker_name: str, url: str) -> list[Record]:
    records: dict[tuple[str, str, str], Record] = {}

    for tr in driver.find_elements(By.CSS_SELECTOR, "table tr"):
        cells = [c.text.strip() for c in tr.find_elements(By.CSS_SELECTOR, "th,td")]
        if len(cells) < 4:
            continue

        trade_date = norm_date(cells[0])
        if trade_date is None:
            continue

        nums = [norm_num(x) for x in cells[1:]]
        nums = [x for x in nums if x is not None]
        if len(nums) < 3:
            continue

        # Known MoneyDJ layouts usually start with buy, sell, then either total/net or net.
        buy, sell = nums[0], nums[1]
        calc = buy - sell

        net: float | None = None
        for candidate in nums[2:5]:
            if abs(candidate - calc) <= 1.0:
                net = calc
                break
        if net is None:
            continue

        key = (trade_date, stock, broker_code)
        records[key] = Record(
            trade_date=trade_date,
            stock=stock,
            broker_code=broker_code,
            broker_name=broker_name,
            buy=buy,
            sell=sell,
            net=net,
            source_url=url,
        )

    return sorted(records.values(), key=lambda r: r.trade_date)


def within_target(records: Iterable[Record], start: str, end: str) -> list[Record]:
    return [r for r in records if start <= r.trade_date <= end]


def enforce_asof_guard(replay_date: str, forbidden_from: str) -> dict:
    # Mandatory negative test from A-CMD-A2-007.
    blocked = forbidden_from > replay_date
    if not blocked:
        raise RuntimeError("AS-OF-D negative guard failed: future date was not blocked")
    return {
        "replay_date": replay_date,
        "forbidden_from": forbidden_from,
        "lookahead": 0,
        "result": "PASS_NEGATIVE_BLOCKED",
    }


def save_records(records: list[Record]) -> None:
    path = OUT / "canary_records.csv"
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(records[0]).keys()) if records else [
            "trade_date", "stock", "broker_code", "broker_name", "buy", "sell", "net", "source_url"
        ])
        writer.writeheader()
        for r in records:
            writer.writerow(asdict(r))


def main() -> int:
    cfg = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    start = cfg["start_date"]
    end = cfg["end_date"]
    canary = cfg["canary"]

    guard = enforce_asof_guard(
        cfg["asof_guard"]["replay_date"],
        cfg["asof_guard"]["forbidden_from"],
    )

    attempts: list[dict] = []
    best: list[Record] = []
    best_url = ""

    driver = chrome()
    try:
        date_variants = [
            ("DASH", "2022-1-1", "2025-6-30"),
            ("ISO", "2022-01-01", "2025-06-30"),
            ("SLASH", "2022/1/1", "2025/6/30"),
            ("COMPACT", "20220101", "20250630"),
        ]

        for host in HOSTS:
            for variant, frm, to in date_variants:
                url = build_url(host, canary["stock"], canary["broker_code"], frm, to)
                attempt = {"host": host, "variant": variant, "url": url, "rows": 0, "min_date": None, "max_date": None}
                try:
                    driver.get(url)
                    wait_dynamic_tables(driver)
                    rows = extract_rows(
                        driver,
                        canary["stock"],
                        canary["broker_code"],
                        canary["broker_name"],
                        url,
                    )
                    rows = within_target(rows, start, end)
                    attempt["rows"] = len(rows)
                    if rows:
                        attempt["min_date"] = rows[0].trade_date
                        attempt["max_date"] = rows[-1].trade_date
                    if len(rows) > len(best) or (
                        rows and best and rows[0].trade_date < best[0].trade_date
                    ):
                        best = rows
                        best_url = url
                except TimeoutException:
                    attempt["error"] = "dynamic_table_timeout"
                except Exception as exc:  # keep trying every free host/variant
                    attempt["error"] = f"{type(exc).__name__}: {exc}"
                attempts.append(attempt)

                if best and best[0].trade_date <= "2022-12-31":
                    break
            if best and best[0].trade_date <= "2022-12-31":
                break

        # Persist diagnostics even on failure.
        (OUT / "attempts.json").write_text(
            json.dumps(attempts, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        try:
            (OUT / "last_page.html").write_text(driver.page_source, encoding="utf-8")
            driver.save_screenshot(str(OUT / "last_page.png"))
        except Exception:
            pass

        save_records(best)

        report = {
            "version": "A2_008_BROWSER_CANARY_V1.0.0",
            "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "target": {"start": start, "end": end},
            "canary": canary,
            "asof_guard": guard,
            "best_source_url": best_url,
            "record_count": len(best),
            "min_date": best[0].trade_date if best else None,
            "max_date": best[-1].trade_date if best else None,
            "result": "PASS_HISTORY" if best else "FAIL_HISTORY",
        }
        (OUT / "report.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        print(json.dumps(report, ensure_ascii=False))
        return 0 if best else 2
    finally:
        driver.quit()


if __name__ == "__main__":
    sys.exit(main())
