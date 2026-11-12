# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 90 · SCD Type 2 live demo (for the interview)
# MAGIC The public HAPI server rarely changes a record inside a 3-day window, so to *prove* the versioning logic we replay a **real Patient
# MAGIC with a changed address and phone** through the normal pipeline (landing → Bronze → Silver) as a clearly-labelled `SIMULATED://` source.
# MAGIC Then we show the history rows. Nothing is written to the public server.

# COMMAND ----------

import os, sys, json
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))
from datetime import datetime, timezone, date
from fhir_lakehouse.config import load_config
from fhir_lakehouse.extract import raw_dir, raw_file_name
from fhir_lakehouse.audit import API_CALL_SCHEMA, log_api_calls
from fhir_lakehouse.ingest import ingest_resource
from fhir_lakehouse.silver import run_silver
cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")))

# COMMAND ----------

# 1) pick a current patient and show its history BEFORE
row = spark.table(cfg.table("silver", "patient")).where("is_current AND city IS NOT NULL").orderBy("patient_id").first()
pid = row.patient_id
display(spark.sql(f"SELECT patient_id, scd_version, valid_from, valid_to, is_current, city, phone FROM {cfg.table('silver','patient')} WHERE patient_id = '{pid}' ORDER BY valid_from"))

# COMMAND ----------

# 2) craft version N+1 of that resource (same id, changed attributes, newer meta.lastUpdated)
src = json.loads(spark.table(cfg.table("bronze", "Patient")).where(f"resource_id = '{pid}'").orderBy("last_updated", ascending=False).first().resource_json)
now = datetime.now(timezone.utc)
src["address"] = [{**(src.get("address") or [{}])[0], "city": "Hyderabad (moved)", "postalCode": "500081"}]
src["telecom"] = [{"system": "phone", "value": "+91-40-0000-0000"}]
src["meta"] = {**src.get("meta", {}), "versionId": str(int(src.get("meta", {}).get("versionId", "1")) + 1),
               "lastUpdated": now.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")}
bundle = {"resourceType": "Bundle", "type": "searchset", "entry": [{"fullUrl": f"SIMULATED/Patient/{pid}", "resource": src, "search": {"mode": "match"}}]}

run_id = f"demo-{now:%H%M%S}"
ld = date.today()
d = raw_dir(cfg, "Patient", ld.isoformat()); os.makedirs(d, exist_ok=True)
fname = raw_file_name(run_id, "Patient", ld.isoformat(), 1)
open(f"{d}/{fname}", "w").write(json.dumps(bundle))
call = dict(run_id=run_id, resource="Patient", load_date=ld.isoformat(), window_start="n/a", window_end="n/a", page_no=1,
            request_url=f"SIMULATED://hapi/Patient/{pid}", http_status=200, attempts=1, entry_count=1,
            bytes=len(json.dumps(bundle)), duration_ms=0, called_at=now.replace(tzinfo=None), raw_file_name=fname, saved_at=now.replace(tzinfo=None))
open(f"{d}/_calls__{run_id}__patient__{ld.isoformat()}.jsonl", "w").write(json.dumps(call, default=str) + "\n")

print(ingest_resource(spark, cfg, "Patient", ld, run_id, mode="daily", source="landing"))
print(run_silver(spark, cfg, "Patient", run_id))

# COMMAND ----------

# 3) history AFTER: old row closed (valid_to set, is_current=false), new row current with scd_version+1
display(spark.sql(f"SELECT patient_id, scd_version, valid_from, valid_to, is_current, city, phone, api_url_or_params FROM {cfg.table('silver','patient')} WHERE patient_id = '{pid}' ORDER BY valid_from"))