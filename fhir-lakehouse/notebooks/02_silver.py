# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 02 · Silver: typed, cleaned, deduplicated, **SCD Type 2**
# MAGIC Incremental by `bronze_loaded_at` watermark. Rows that cannot be loaded (no id / no lastUpdated) go to `silver.dq_quarantine` instead of being dropped.
# MAGIC Change detection = SHA-256 over business attributes; a changed row closes the old version (`valid_to`, `is_current=false`) and inserts a new one (`scd_version+1`).

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))   # repo/src on the path
from fhir_lakehouse.config import load_config
cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")))
from uuid import uuid4
from fhir_lakehouse.silver import run_silver

dbutils.widgets.text("run_id", "")
run_id = dbutils.widgets.get("run_id").strip() or f"manual-{uuid4().hex[:8]}"

for resource in cfg.resources:          # Patient -> Encounter -> Observation -> Condition
    print(resource, run_silver(spark, cfg, resource, run_id))

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {cfg.table('ctrl','pipeline_run_log')} WHERE run_id = '{run_id}' AND stage = 'silver'"))