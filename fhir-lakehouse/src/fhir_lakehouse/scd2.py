"""Generic SCD Type 2 on Delta. History is kept, exactly one is_current row per business key."""
from __future__ import annotations
from typing import List

from pyspark.sql import DataFrame, functions as F

SCD_COLS = ["valid_to", "is_current", "scd_version", "silver_loaded_at"]


def build_staged(source: DataFrame, current: DataFrame | None, key: str) -> tuple[DataFrame, dict]:
    """
    Classic 'merge-key' staging so ONE MERGE both closes the old row and inserts the new one:
      A) every new/changed row with merge_key = business key  -> matches current row (close it) or inserts if brand new
      B) every *changed* row again with merge_key = NULL       -> never matches -> always inserts the new version
    A change is applied only if the hash differs AND the incoming version is newer (guards late/out-of-order data).
    """
    if current is None:
        a = (source.withColumn("merge_key", F.col(key)).withColumn("scd_version", F.lit(1)))
        return a, {"new": source.count(), "changed": 0}
    cur = current.where("is_current").select(
        F.col(key).alias("_k"), F.col("row_hash").alias("_h"),
        F.col("valid_from").alias("_ts"), F.col("scd_version").alias("_v"))
    j = source.join(cur, source[key] == cur["_k"], "left")
    is_new = F.col("_k").isNull()
    is_changed = F.col("_k").isNotNull() & (F.col("row_hash") != F.col("_h")) & (F.col("valid_from") > F.col("_ts"))
    todo = j.where(is_new | is_changed).withColumn(
        "scd_version", F.coalesce(F.col("_v"), F.lit(0)) + 1).withColumn("_chg", is_changed)
    base_cols = [c for c in source.columns] + ["scd_version"]
    a = todo.select(*base_cols, F.col(key).alias("merge_key"))
    b = todo.where("_chg").select(*base_cols, F.lit(None).cast("string").alias("merge_key"))
    stats = {"new": todo.where("not _chg").count(), "changed": todo.where("_chg").count()}
    return a.unionByName(b), stats


def apply_scd2(spark, source: DataFrame, target: str, key: str) -> dict:
    from delta.tables import DeltaTable  # imported lazily so unit tests need no Delta jar
    src = (source.withColumn("valid_to", F.lit(None).cast("timestamp"))
                 .withColumn("is_current", F.lit(True))
                 .withColumn("silver_loaded_at", F.current_timestamp()))
    exists = spark.catalog.tableExists(target)
    staged, stats = build_staged(src, spark.table(target) if exists else None, key)
    if not exists:
        staged.drop("merge_key").write.format("delta").saveAsTable(target)
        return {**stats, "unchanged": 0}
    cols: List[str] = [c for c in staged.columns if c != "merge_key"]
    (DeltaTable.forName(spark, target).alias("t")
        .merge(staged.alias("s"), f"t.{key} = s.merge_key AND t.is_current = true")
        .whenMatchedUpdate(set={"is_current": "false", "valid_to": "s.valid_from"})
        .whenNotMatchedInsert(values={c: f"s.{c}" for c in cols})
        .execute())
    stats["unchanged"] = src.count() - stats["new"] - stats["changed"]
    return stats
