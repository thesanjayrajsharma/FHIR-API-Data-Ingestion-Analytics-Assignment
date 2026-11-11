"""Idempotent DDL: catalog objects, raw Volume and control tables."""
from .audit import API_CALL_SCHEMA, RUN_LOG_SCHEMA, WATERMARK_SCHEMA


def _ddl(schema) -> str:
    return ", ".join(f"{f.name} {f.dataType.simpleString()}" for f in schema.fields)


def create_objects(spark, cfg):
    for layer in ("raw", "bronze", "silver", "gold", "ctrl"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.schema(layer)}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.schema('raw')}.{cfg.raw['storage']['raw_volume']}")
    for name, schema in (("api_call_log", API_CALL_SCHEMA),
                         ("pipeline_run_log", RUN_LOG_SCHEMA),
                         ("watermark", WATERMARK_SCHEMA)):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {cfg.table('ctrl', name)} ({_ddl(schema)}) USING DELTA")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {cfg.table('silver', 'dq_quarantine')} (
        resource STRING, resource_id STRING, reasons STRING, resource_json STRING,
        run_ts TIMESTAMP) USING DELTA""")
