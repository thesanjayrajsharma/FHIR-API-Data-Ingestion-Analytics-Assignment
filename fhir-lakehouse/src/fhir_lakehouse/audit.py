"""Control-plane tables: api_call_log, pipeline_run_log, watermark. Answers 'when was each API called / data saved?'"""
from __future__ import annotations
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, List, Optional

from pyspark.sql import SparkSession, types as T

from .util import utcnow

API_CALL_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("resource", T.StringType()),
    T.StructField("load_date", T.StringType()),
    T.StructField("window_start", T.StringType()),
    T.StructField("window_end", T.StringType()),
    T.StructField("page_no", T.IntegerType()),
    T.StructField("request_url", T.StringType()),
    T.StructField("http_status", T.IntegerType()),
    T.StructField("attempts", T.IntegerType()),
    T.StructField("entry_count", T.IntegerType()),
    T.StructField("bytes", T.LongType()),
    T.StructField("duration_ms", T.LongType()),
    T.StructField("called_at", T.TimestampType()),      # when the API was called
    T.StructField("raw_file_name", T.StringType()),
    T.StructField("saved_at", T.TimestampType()),       # when the response was persisted
])

RUN_LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("stage", T.StringType()),             # extract | bronze | silver | gold
    T.StructField("resource", T.StringType()),
    T.StructField("status", T.StringType()),            # SUCCESS | FAILED
    T.StructField("rows_in", T.LongType()),
    T.StructField("rows_out", T.LongType()),
    T.StructField("details", T.StringType()),
    T.StructField("started_at", T.TimestampType()),
    T.StructField("ended_at", T.TimestampType()),
    T.StructField("error", T.StringType()),
])

WATERMARK_SCHEMA = T.StructType([
    T.StructField("scope", T.StringType()),             # e.g. api:Patient, silver:Patient
    T.StructField("watermark_ts", T.TimestampType()),
    T.StructField("run_id", T.StringType()),
    T.StructField("updated_at", T.TimestampType()),
])


def _append(spark: SparkSession, rows: List[dict], schema: T.StructType, table: str):
    if not rows:
        return
    df = spark.createDataFrame([tuple(r.get(f.name) for f in schema.fields) for r in rows], schema)
    df.write.format("delta").mode("append").saveAsTable(table)


def log_api_calls(spark, cfg, rows: List[dict]):
    _append(spark, rows, API_CALL_SCHEMA, cfg.table("ctrl", "api_call_log"))


class StageRun:
    """Context manager that writes exactly one row to pipeline_run_log, success or failure."""

    def __init__(self, spark, cfg, run_id: str, stage: str, resource: Optional[str]):
        self.spark, self.cfg = spark, cfg
        self.row: Dict = dict(run_id=run_id, stage=stage, resource=resource, status="RUNNING",
                              rows_in=None, rows_out=None, details=None, error=None)

    def set(self, rows_in=None, rows_out=None, details=None):
        if rows_in is not None: self.row["rows_in"] = int(rows_in)
        if rows_out is not None: self.row["rows_out"] = int(rows_out)
        if details is not None: self.row["details"] = str(details)

    def __enter__(self):
        self.row["started_at"] = utcnow()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.row["ended_at"] = utcnow()
        self.row["status"] = "FAILED" if exc else "SUCCESS"
        if exc:
            self.row["error"] = "".join(traceback.format_exception_only(exc_type, exc))[:4000]
        _append(self.spark, [self.row], RUN_LOG_SCHEMA, self.cfg.table("ctrl", "pipeline_run_log"))
        return False  # never swallow the exception


def get_watermark(spark, cfg, scope: str):
    tbl = cfg.table("ctrl", "watermark")
    row = spark.table(tbl).where(f"scope = '{scope}'").select("watermark_ts").first()
    return row[0] if row else None


def set_watermark(spark, cfg, scope: str, ts: datetime, run_id: str):
    tbl = cfg.table("ctrl", "watermark")
    src = spark.createDataFrame([(scope, ts, run_id, utcnow())], WATERMARK_SCHEMA)
    src.createOrReplaceTempView("_wm_src")
    spark.sql(f"""
        MERGE INTO {tbl} t USING _wm_src s ON t.scope = s.scope
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *""")
