"""Validates every Gold SQL statement parses and runs against tiny local tables (parquet stands in for Delta)."""
import json
from pyspark.sql import functions as F
from fhir_lakehouse import gold
from fhir_lakehouse.silver_transform import flatten
from test_transform_scd2 import PATIENT, OBS, ENC, COND, bronze


def _silver(spark, res, doc, name, db):
    df = flatten(bronze(spark, doc), res).drop("fatal_reasons")
    df = (df.withColumn("valid_to", F.lit(None).cast("timestamp")).withColumn("is_current", F.lit(True))
            .withColumn("scd_version", F.lit(1)).withColumn("silver_loaded_at", F.current_timestamp()))
    df.write.mode("overwrite").saveAsTable(f"silver.{name}")


def test_all_gold_sql_runs(spark):
    for db in ("silver", "gold", "ctrl"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
    for res, doc in (("Patient", PATIENT), ("Encounter", ENC), ("Observation", OBS), ("Condition", COND)):
        _silver(spark, res, doc, res.lower(), "silver")
    spark.sql("CREATE TABLE IF NOT EXISTS ctrl.pipeline_run_log (run_id string, stage string, resource string, status string, rows_in long, rows_out long, details string, started_at timestamp, ended_at timestamp, error string) USING parquet")
    spark.sql("CREATE TABLE IF NOT EXISTS ctrl.api_call_log (resource string, load_date string, entry_count int, bytes long, called_at timestamp, saved_at timestamp, attempts int, duration_ms long) USING parquet")
    spark.sql("CREATE TABLE IF NOT EXISTS silver.dq_quarantine (resource string, resource_id string, reasons string, resource_json string, run_ts timestamp) USING parquet")
    fmt = dict(silver="silver", gold="gold", ctrl="ctrl")
    for name, sql in gold.TABLES.items():            # same order as production; CTAS emulated with saveAsTable
        spark.sql(sql.format(**fmt)).write.mode("overwrite").saveAsTable(f"gold.{name}")
    for name, sql in gold.VIEWS.items():
        spark.sql(f"CREATE OR REPLACE VIEW gold.{name} AS {sql.format(**fmt)}")
        spark.table(f"gold.{name}").collect()
    p = spark.table("gold.vw_patient_360").first()
    assert p.encounters == 1 and p.observations == 1 and p.conditions == 1
    assert spark.table("gold.fact_observation").first().is_orphan_patient is False
    assert spark.table("gold.dim_date").count() > 1000
