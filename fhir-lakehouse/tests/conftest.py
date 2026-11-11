import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import pytest

@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession
    wh = str(tmp_path_factory.mktemp("warehouse"))
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false").config("spark.sql.warehouse.dir", wh)
         .config("spark.sql.ansi.enabled", "true")      # mimic Databricks serverless (ANSI on)
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()
