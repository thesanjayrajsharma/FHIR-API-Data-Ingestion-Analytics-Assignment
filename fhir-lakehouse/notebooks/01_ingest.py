# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Ingest one resource: API → Raw → Bronze
# MAGIC * **Raw**: every Bundle page saved byte-for-byte to `/Volumes/.../landing/<resource>/load_date=YYYY-MM-DD/`
# MAGIC * **Bronze**: one Delta row per resource *version* (insert-only MERGE), with `extraction_timestamp` and `api_url_or_params`
# MAGIC * Logs every HTTP call (`ctrl.api_call_log`) and every stage (`ctrl.pipeline_run_log`)
# MAGIC
# MAGIC Parameters: `resource`, `load_date` (blank = yesterday UTC), `mode` (daily|incremental), `source` (api|landing), `run_id` (blank = generated)

# COMMAND ----------

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))   # repo/src on the path
from fhir_lakehouse.config import load_config
cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")))
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4
from fhir_lakehouse.ingest import ingest_resource

dbutils.widgets.dropdown("resource", "Patient", cfg.resources)
dbutils.widgets.text("load_date", "")
dbutils.widgets.dropdown("mode", "daily", ["daily", "incremental"])
dbutils.widgets.dropdown("source", "api", ["api", "landing"])
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("max_pages", "")        # optional dev cap (pages per resource per day)

resource = dbutils.widgets.get("resource")
ld = dbutils.widgets.get("load_date").strip()
load_date = date.fromisoformat(ld) if ld else (datetime.now(timezone.utc) - timedelta(days=1)).date()
run_id = dbutils.widgets.get("run_id").strip() or f"manual-{uuid4().hex[:8]}"
mp = dbutils.widgets.get("max_pages").strip()
if mp:
    cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")),
                      overrides={"fhir.max_pages_per_run": int(mp)})

# COMMAND ----------

result = ingest_resource(spark, cfg, resource, load_date, run_id,
                         mode=dbutils.widgets.get("mode"), source=dbutils.widgets.get("source"))
print(result)

# COMMAND ----------

display(spark.sql(f"""
  SELECT stage, resource, status, rows_in, rows_out, details, started_at, ended_at
  FROM {cfg.table('ctrl','pipeline_run_log')} WHERE run_id = '{run_id}' AND resource = '{resource}' ORDER BY started_at"""))

# COMMAND ----------

