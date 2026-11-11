"""Bronze -> Silver: incremental (by bronze_loaded_at watermark), cleaned, deduplicated, SCD2-versioned."""
from __future__ import annotations
from pyspark.sql import Window, functions as F

from .audit import StageRun, get_watermark, set_watermark, utcnow
from .scd2 import apply_scd2
from .silver_transform import BUSINESS_KEY, flatten


def run_silver(spark, cfg, resource: str, run_id: str) -> dict:
    key = BUSINESS_KEY[resource]
    scope = f"silver:{resource}"
    with StageRun(spark, cfg, run_id, "silver", resource) as st:
        wm = get_watermark(spark, cfg, scope)
        bronze = spark.table(cfg.table("bronze", resource))
        if wm is not None:
            bronze = bronze.where(F.col("bronze_loaded_at") > F.lit(wm))
        bronze = bronze.cache()
        n_in = bronze.count()
        if n_in == 0:
            st.set(rows_in=0, rows_out=0, details="no new bronze rows")
            return {"rows_in": 0}
        flat = flatten(bronze, resource).cache()

        # 1) quarantine un-loadable rows (kept, never silently dropped)
        bad = flat.where("fatal_reasons != ''")
        (bad.select(F.lit(resource).alias("resource"), F.col(key).alias("resource_id"),
                    F.col("fatal_reasons").alias("reasons"), "resource_json",
                    F.current_timestamp().alias("run_ts"))
            .write.format("delta").mode("append").saveAsTable(cfg.table("silver", "dq_quarantine")))
        good = flat.where("fatal_reasons = ''").drop("fatal_reasons")

        # 2) dedupe: same key + same version re-delivered -> keep the latest extraction
        w = Window.partitionBy(key, "valid_from").orderBy(F.col("extraction_timestamp").desc_nulls_last())
        good = good.withColumn("_r", F.row_number().over(w)).where("_r = 1").drop("_r")

        # 3) apply versions oldest->newest so multiple versions in one batch still build correct history
        order = Window.partitionBy(key).orderBy("valid_from")
        good = good.withColumn("_seq", F.row_number().over(order)).cache()
        max_seq = good.agg(F.max("_seq")).first()[0] or 0
        totals = {"new": 0, "changed": 0, "unchanged": 0}
        for seq in range(1, max_seq + 1):
            stats = apply_scd2(spark, good.where(F.col("_seq") == seq).drop("_seq"),
                               cfg.table("silver", resource), key)
            for k in totals:
                totals[k] += stats.get(k, 0)

        newest = bronze.agg(F.max("bronze_loaded_at")).first()[0]
        set_watermark(spark, cfg, scope, newest, run_id)
        st.set(rows_in=n_in, rows_out=totals["new"] + totals["changed"],
               details=f"new={totals['new']} changed={totals['changed']} unchanged={totals['unchanged']} "
                       f"quarantined={bad.count()}")
        return {"rows_in": n_in, **totals}
