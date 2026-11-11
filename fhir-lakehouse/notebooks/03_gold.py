# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold: star schema + reporting views
# MAGIC Dims: `dim_patient`, `dim_date`, `dim_clinical_code` · Facts: `fact_encounter`, `fact_observation`, `fact_condition`
# MAGIC Views: `vw_patient_360`, `vw_encounters_daily`, `vw_top_conditions`, `vw_vitals`, `vw_patient_history`, `vw_scd2_change_audit`, `vw_pipeline_health`, `vw_api_call_audit`, `vw_data_quality`

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))   # repo/src on the path
from fhir_lakehouse.config import load_config
cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")))
from uuid import uuid4
from fhir_lakehouse.gold import build_gold

dbutils.widgets.text("run_id", "")
run_id = dbutils.widgets.get("run_id").strip() or f"manual-{uuid4().hex[:8]}"
build_gold(spark, cfg, run_id)

# COMMAND ----------
for t in ["dim_patient", "dim_date", "dim_clinical_code", "fact_encounter", "fact_observation", "fact_condition"]:
    print(f"{t:<20}", spark.table(f"{cfg.schema('gold')}.{t}").count())
