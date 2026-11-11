"""End-to-end local check: fake API -> raw files (byte-identical) -> bronze dataframe with lineage metadata."""
import json, os
from datetime import date
from fhir_lakehouse.config import Config, load_config
from fhir_lakehouse.extract import extract_resource
from fhir_lakehouse.fhir_client import FhirClient
from fhir_lakehouse.windows import compute_window
from fhir_lakehouse.bronze import build_bronze_df
from fhir_lakehouse.audit import API_CALL_SCHEMA
from test_windows_and_client import FakeResp, FakeSession, bundle


def test_raw_is_byte_identical_and_bronze_has_metadata(spark, tmp_path):
    class LocalCfg(Config):
        raw_root = str(tmp_path)
        def schema(self, layer): return self.schemas[layer]
    cfg = LocalCfg(load_config().raw)
    first, second = bundle(2, "http://p2"), bundle(1)
    second["entry"][0]["resource"]["id"] = "9"      # distinct id (identical payloads are de-duplicated by design)
    sess = FakeSession([FakeResp(first, url="http://base/Patient?_count=100"), FakeResp(second, url="http://p2")])
    client = FhirClient("http://base", session=sess, sleep=lambda _: None)
    w = compute_window("daily", date(2026, 11, 9))
    res = extract_resource(client, cfg, "Patient", w, "run1")
    assert (res.pages, res.entries) == (2, 3)
    f = tmp_path / "patient" / "load_date=2026-11-09" / "run1__patient__2026-11-09__p00001.json"
    assert f.read_bytes() == json.dumps(first).encode()                       # as-is
    assert (tmp_path / "patient" / "load_date=2026-11-09" / "_calls__run1__patient__2026-11-09.jsonl").exists()

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.schemas['ctrl']}")
    spark.createDataFrame([tuple(r.get(x.name) for x in API_CALL_SCHEMA.fields) for r in res.call_rows], API_CALL_SCHEMA) \
        .write.mode("overwrite").saveAsTable(cfg.table("ctrl", "api_call_log"))
    df = build_bronze_df(spark, cfg, "Patient", "run1", "2026-11-09")
    rows = df.collect()
    assert len(rows) == 3
    assert all(r.api_url_or_params and r.extraction_timestamp and r.payload_hash for r in rows)
    assert {r.load_date for r in rows} == {"2026-11-09"}
