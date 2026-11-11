# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Validation & evidence
# MAGIC Row counts per layer, SCD2 integrity (exactly one current row per key, no overlapping validity), orphan foreign keys, API call audit.
# MAGIC Fails the job if an integrity rule is broken.

# COMMAND ----------
import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.getcwd(), "..", "src")))   # repo/src on the path
from fhir_lakehouse.config import load_config
cfg = load_config(os.path.abspath(os.path.join(os.getcwd(), "..", "config", "pipeline_config.yml")))
from fhir_lakehouse.silver_transform import BUSINESS_KEY

problems = []
rows = []
for r in cfg.resources:
    k = BUSINESS_KEY[r]
    b = spark.table(cfg.table("bronze", r)).count()
    s_all = spark.table(cfg.table("silver", r)).count()
    s_cur = spark.table(cfg.table("silver", r)).where("is_current").count()
    multi = spark.sql(f"SELECT {k} FROM {cfg.table('silver', r)} WHERE is_current GROUP BY {k} HAVING count(*) > 1").count()
    overlap = spark.sql(f"""
      SELECT 1 FROM (SELECT {k}, valid_from, valid_to, lead(valid_from) OVER (PARTITION BY {k} ORDER BY valid_from) nxt
                     FROM {cfg.table('silver', r)}) WHERE valid_to IS NOT NULL AND nxt IS NOT NULL AND valid_to > nxt""").count()
    rows.append((r, b, s_all, s_cur, multi, overlap))
    if multi: problems.append(f"{r}: {multi} keys with >1 current row")
    if overlap: problems.append(f"{r}: {overlap} overlapping validity ranges")
display(spark.createDataFrame(rows, "resource string, bronze_versions long, silver_rows long, silver_current long, keys_with_multi_current long, overlaps long"))

# COMMAND ----------
g = cfg.schema("gold")
display(spark.sql(f"""
 SELECT 'fact_encounter' t, count(*) n, sum(CAST(is_orphan_patient AS INT)) orphans FROM {g}.fact_encounter UNION ALL
 SELECT 'fact_observation', count(*), sum(CAST(is_orphan_patient AS INT)) FROM {g}.fact_observation UNION ALL
 SELECT 'fact_condition', count(*), sum(CAST(is_orphan_patient AS INT)) FROM {g}.fact_condition"""))
display(spark.sql(f"SELECT * FROM {g}.vw_api_call_audit ORDER BY resource, load_date"))
display(spark.sql(f"SELECT * FROM {g}.vw_scd2_change_audit ORDER BY last_changed DESC LIMIT 50"))

# COMMAND ----------
assert not problems, "Integrity check failed: " + "; ".join(problems)
print("All integrity checks passed")
