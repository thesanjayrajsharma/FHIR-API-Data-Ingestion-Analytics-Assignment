# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Backfill driver (the 2–3 day incremental load)
# MAGIC For each day D: **Patient → Encounter → Observation → Condition** (extract+bronze), then Silver → Gold → Validate.
# MAGIC Uses `dbutils.notebook.run`, so it works even where Jobs are unavailable. The Workflow in `workflows/` does the same thing as a DAG.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))   # repo/src on the path
from fhir_lakehouse.config import load_config
cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")))
from datetime import date, timedelta
from uuid import uuid4

dbutils.widgets.text("from_date", "")      # blank = today-3d
dbutils.widgets.text("days", "3")
dbutils.widgets.text("max_pages", "")      # optional dev cap per resource per day, e.g. 25
dbutils.widgets.dropdown("source", "api", ["api", "landing"])
dbutils.widgets.text("landing_run_id", "")  # required when source=landing (the run_id used by local/extract_local.py)

days = int(dbutils.widgets.get("days"))
fd = dbutils.widgets.get("from_date").strip()
start = date.fromisoformat(fd) if fd else date.today() - timedelta(days=days)
source = dbutils.widgets.get("source")
cap = dbutils.widgets.get("max_pages").strip()

NB = {"ingest": "./01_ingest", "silver": "./02_silver", "gold": "./03_gold", "validate": "./04_validate"}
TIMEOUT = 3600

# COMMAND ----------
for i in range(days):
    d = (start + timedelta(days=i)).isoformat()
    run_id = dbutils.widgets.get("landing_run_id").strip() if source == "landing" else f"bf-{d.replace('-', '')}-{uuid4().hex[:6]}"
    print(f"===== {d}  run_id={run_id} =====")
    for res in cfg.resources:                                   # strict order
        dbutils.notebook.run(NB["ingest"], TIMEOUT, {"resource": res, "load_date": d, "mode": "daily", "source": source, "run_id": run_id, "max_pages": cap})
    dbutils.notebook.run(NB["silver"], TIMEOUT, {"run_id": run_id})
dbutils.notebook.run(NB["gold"], TIMEOUT, {"run_id": run_id})
dbutils.notebook.run(NB["validate"], TIMEOUT, {})
print("Backfill complete")
