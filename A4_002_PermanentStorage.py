from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Asia/Taipei")
VERSION = "A4_002_STORAGE_V1.0.0"


def now_tw() -> datetime:
    return datetime.now(TZ)


def iter_ndjson(path: Path) -> Iterable[dict[str, Any]]:
    with path.open("r", encoding="utf-8") as fh:
        for lineno, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                raise ValueError(f"line {lineno} is not an object")
            yield obj


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_raw_object_name(source_date: str, run_id: str, source: str, filename: str) -> str:
    safe_source = source.upper().replace(" ", "_")
    return f"a4/raw/source_date={source_date}/source={safe_source}/run_id={run_id}/{filename}"


def build_clean_table_spec(project: str, dataset: str, table: str) -> dict[str, Any]:
    return {
        "table": f"{project}.{dataset}.{table}",
        "partition_field": "SOURCE_DATE",
        "cluster_fields": ["code", "SOURCE", "phase"],
        "dedup_key": "DATA_KEY",
    }


def dry_run_manifest(raw_path: Path, source_date: str, run_id: str, source: str) -> dict[str, Any]:
    if not raw_path.exists() or raw_path.stat().st_size == 0:
        raise FileNotFoundError(f"raw file missing/empty: {raw_path}")
    count = sum(1 for _ in iter_ndjson(raw_path))
    return {
        "component": "A4_002",
        "version": VERSION,
        "checked_at": now_tw().isoformat(),
        "source_date": source_date,
        "run_id": run_id,
        "source": source,
        "rows": count,
        "bytes": raw_path.stat().st_size,
        "sha256": sha256_file(raw_path),
        "gcs_object": build_raw_object_name(source_date, run_id, source, raw_path.name),
        "pass": count > 0,
    }


def upload_raw_to_gcs(raw_path: Path, bucket_name: str, object_name: str) -> None:
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(str(raw_path), if_generation_match=0)


def load_clean_to_bigquery(clean_path: Path, project: str, dataset: str, table: str) -> None:
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    table_id = f"{project}.{dataset}.{table}"
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
        time_partitioning=bigquery.TimePartitioning(field="SOURCE_DATE"),
        clustering_fields=["code", "SOURCE", "phase"],
    )
    with clean_path.open("rb") as fh:
        job = client.load_table_from_file(fh, table_id, job_config=job_config)
    job.result()


def self_test() -> int:
    root = Path("output/a4_002_selftest")
    root.mkdir(parents=True, exist_ok=True)
    raw = root / "raw_test.ndjson"
    rows = [
        {"captured_at":"2026-08-17T08:30:00+08:00","phase":"PREOPEN","raw":{"msgArray":[{"c":"2330"}]}},
        {"captured_at":"2026-08-17T08:30:05+08:00","phase":"PREOPEN","raw":{"msgArray":[{"c":"2317"}]}},
    ]
    raw.write_text("\n".join(json.dumps(x, ensure_ascii=False) for x in rows)+"\n", encoding="utf-8")
    manifest = dry_run_manifest(raw, "2026-08-17", "RUN123", "TWSE_MIS")
    spec = build_clean_table_spec("qts-test", "qts_a4", "preopen_clean")
    checks = {
        "rows_counted": manifest["rows"] == 2,
        "sha256_present": len(manifest["sha256"]) == 64,
        "immutable_object_path": manifest["gcs_object"].startswith("a4/raw/source_date=2026-08-17/source=TWSE_MIS/run_id=RUN123/"),
        "partition_field": spec["partition_field"] == "SOURCE_DATE",
        "cluster_fields": spec["cluster_fields"] == ["code", "SOURCE", "phase"],
        "dedup_key": spec["dedup_key"] == "DATA_KEY",
        "no_cloud_write_in_selftest": True,
    }
    report = {"component":"A4_002","version":VERSION,"mode":"self_test","checked_at":now_tw().isoformat(),"checks":checks,"pass":all(checks.values())}
    Path("status/a4").mkdir(parents=True, exist_ok=True)
    Path("status/a4/a4_002_storage_selftest_latest.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["pass"] else 2


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--self-test", action="store_true")
    p.add_argument("--raw")
    p.add_argument("--clean")
    p.add_argument("--source-date")
    p.add_argument("--run-id")
    p.add_argument("--source", default="TWSE_MIS")
    p.add_argument("--upload", action="store_true")
    args = p.parse_args()
    if args.self_test:
        return self_test()

    required = [args.raw, args.source_date, args.run_id]
    if not all(required):
        p.error("--raw --source-date --run-id are required")

    raw_path = Path(args.raw)
    manifest = dry_run_manifest(raw_path, args.source_date, args.run_id, args.source)
    print(json.dumps(manifest, ensure_ascii=False))

    if not args.upload:
        return 0

    project = os.environ["GCP_PROJECT_ID"]
    bucket = os.environ["A4_GCS_BUCKET"]
    dataset = os.environ["A4_BQ_DATASET"]
    table = os.getenv("A4_BQ_CLEAN_TABLE", "preopen_clean")

    upload_raw_to_gcs(raw_path, bucket, manifest["gcs_object"])
    if args.clean:
        load_clean_to_bigquery(Path(args.clean), project, dataset, table)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        print(json.dumps({"component":"A4_002","pass":False,"error":f"{type(exc).__name__}: {exc}"}, ensure_ascii=False), file=sys.stderr)
        sys.exit(99)
