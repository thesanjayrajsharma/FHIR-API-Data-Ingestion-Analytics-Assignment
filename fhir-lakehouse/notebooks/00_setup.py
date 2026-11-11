# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates schemas (raw/bronze/silver/gold/ctrl), the raw **Volume**, and the control tables. Safe to re-run.
# MAGIC
# MAGIC **Smoke test for Free Edition:** the last cell checks whether the workspace can reach the FHIR server.
# MAGIC If it cannot, use the offline path described in the README (§ "If outbound internet is blocked").

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))   # repo/src on the path
from fhir_lakehouse.config import load_config
cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")))
from fhir_lakehouse.setup import create_objects
create_objects(spark, cfg)
display(spark.sql(f"SHOW SCHEMAS IN {cfg.catalog}").where("databaseName like 'fhir_%'"))

# COMMAND ----------
# Connectivity smoke test (fails fast with a clear message)
import requests
try:
    r = requests.get(f"{cfg.fhir['base_url']}/metadata", params={"_summary": "true"}, timeout=20)
    print("FHIR server reachable:", r.status_code, "| software:", r.json().get("software", {}).get("name"))
except Exception as e:
    print("CANNOT reach FHIR server from this workspace ->", type(e).__name__)
    print("Use the offline path (local/extract_local.py + source=landing). See README.")
