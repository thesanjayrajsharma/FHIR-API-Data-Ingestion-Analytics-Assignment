"""Raw Bundle files -> Bronze Delta (one row per resource version, immutable, with lineage metadata)."""
from __future__ import annotations
import json
import os
from typing import List

from pyspark.sql import DataFrame, functions as F, types as T

from .audit import API_CALL_SCHEMA

_ENTRY_T = T.ArrayType(T.StructType([
    T.StructField("full_url", T.StringType()),
    T.StructField("resource_json", T.StringType()),
]))


def make_entries_udf(resource: str):
    """Explode a Bundle body into one JSON string per matching resource (drops OperationOutcome etc.)."""
    def _entries(body):
        if not body:
            return []
        out = []
        for e in json.loads(body).get("entry", []) or []:
            r = e.get("resource") or {}
            mode = (e.get("search") or {}).get("mode", "match")
            if r.get("resourceType") == resource and mode == "match":
                out.append((e.get("fullUrl"), json.dumps(r, separators=(",", ":"), ensure_ascii=False)))
        return out
    return F.udf(_entries, _ENTRY_T)


def load_call_sidecars(spark, cfg, resource: str, run_id: str, load_date: str = "*") -> DataFrame:
    """Read api_call_log rows for this run (from the Delta log table)."""
    q = (spark.table(cfg.table("ctrl", "api_call_log"))
         .where((F.col("run_id") == run_id) & (F.col("resource") == resource)))
    return q if load_date == "*" else q.where(F.col("load_date") == load_date)


def build_bronze_df(spark, cfg, resource: str, run_id: str, load_date: str = "*") -> DataFrame:
    pattern = f"{cfg.raw_root}/{resource.lower()}/load_date={load_date}/{run_id}__*.json"
    files = (spark.read.format("text").option("wholetext", True).load(pattern)
             .select(F.col("value").alias("body"),
                     F.col("_metadata.file_name").alias("source_file"),
                     F.col("_metadata.file_path").alias("file_path")))
    calls = load_call_sidecars(spark, cfg, resource, run_id, load_date).select(
        F.col("raw_file_name").alias("source_file"),
        F.col("request_url").alias("api_url_or_params"),
        F.col("called_at").alias("extraction_timestamp"))
    df = (files.join(calls, "source_file", "left")
          .withColumn("e", F.explode(make_entries_udf(resource)("body")))
          .select(
              F.lit(resource).alias("resource_type"),
              F.get_json_object("e.resource_json", "$.id").alias("resource_id"),
              F.get_json_object("e.resource_json", "$.meta.versionId").alias("source_version_id"),
              F.expr("try_cast(get_json_object(e.resource_json, '$.meta.lastUpdated') as timestamp)").alias("last_updated"),
              F.sha2(F.col("e.resource_json"), 256).alias("payload_hash"),
              F.col("e.resource_json").alias("resource_json"),
              "extraction_timestamp", "api_url_or_params", "source_file",
              F.regexp_extract("file_path", r"load_date=([0-9\-]+)", 1).alias("load_date"),
              F.lit(run_id).alias("run_id"),
              F.current_timestamp().alias("bronze_loaded_at")))
    return df.dropDuplicates(["resource_id", "payload_hash"])


def merge_bronze(spark, cfg, resource: str, df: DataFrame) -> int:
    """Insert-only MERGE: identical payloads are never duplicated, every distinct version is kept forever."""
    tbl = cfg.table("bronze", resource)
    if not spark.catalog.tableExists(tbl):
        df.write.format("delta").saveAsTable(tbl)
        return df.count()
    before = spark.table(tbl).count()
    df.createOrReplaceTempView("_bronze_src")
    spark.sql(f"""
        MERGE INTO {tbl} t USING _bronze_src s
        ON t.resource_id = s.resource_id AND t.payload_hash = s.payload_hash
        WHEN NOT MATCHED THEN INSERT *""")
    return spark.table(tbl).count() - before
