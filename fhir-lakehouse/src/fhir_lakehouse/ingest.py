"""Extract + Bronze for one resource. Used by notebook 01 and by the job task."""
from __future__ import annotations
import glob
import json
import os
from datetime import date, datetime

from pyspark.sql import functions as F

from .audit import StageRun, get_watermark, log_api_calls, set_watermark
from .bronze import build_bronze_df, merge_bronze
from .extract import extract_resource, raw_dir
from .fhir_client import FhirClient
from .windows import compute_window


def make_client(cfg) -> FhirClient:
    f = cfg.fhir
    return FhirClient(f["base_url"], f["page_size"], f["timeout_seconds"], f["max_retries"],
                      f["backoff_seconds"], f.get("accept", "application/fhir+json"))


def ingest_resource(spark, cfg, resource: str, load_date: date, run_id: str,
                    mode: str = "daily", source: str = "api") -> dict:
    """
    source='api'     -> call the FHIR server.
    source='landing' -> files were produced elsewhere (local/extract_local.py) and uploaded to the Volume;
                        we register their call log and continue with Bronze. Same downstream path.
    """
    scope = f"api:{resource}"
    window = compute_window(mode, load_date, get_watermark(spark, cfg, scope))
    result = {"resource": resource, "window": f"{window.start_s} .. {window.end_s}"}

    with StageRun(spark, cfg, run_id, "extract", resource) as st:
        if source == "api":
            res = extract_resource(make_client(cfg), cfg, resource, window, run_id)
            rows = res.call_rows
        else:
            rows = []
            for f in glob.glob(f"{raw_dir(cfg, resource, window.load_date)}/_calls__{run_id}__*__{window.load_date}.jsonl"):
                with open(f) as fh:
                    for line in fh:
                        r = json.loads(line)
                        for t in ("called_at", "saved_at"):
                            r[t] = datetime.fromisoformat(r[t])
                        rows.append(r)
        log_api_calls(spark, cfg, rows)
        st.set(rows_in=len(rows), rows_out=sum(r["entry_count"] for r in rows),
               details=f"pages={len(rows)} window={result['window']}")
    result["pages"] = len(rows)
    if not rows or sum(r["entry_count"] for r in rows) == 0:
        result["bronze_rows"] = 0
        return result

    with StageRun(spark, cfg, run_id, "bronze", resource) as st:
        df = build_bronze_df(spark, cfg, resource, run_id, window.load_date).cache()
        n = merge_bronze(spark, cfg, resource, df)
        newest = df.agg(F.max("last_updated")).first()[0]
        if newest is not None:
            set_watermark(spark, cfg, scope, newest, run_id)
        st.set(rows_in=df.count(), rows_out=n, details="insert-only merge on (resource_id, payload_hash)")
    result["bronze_rows"] = n
    return result
