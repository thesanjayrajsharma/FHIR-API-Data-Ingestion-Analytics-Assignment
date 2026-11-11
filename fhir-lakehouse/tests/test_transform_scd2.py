import json
from pyspark.sql import functions as F
from fhir_lakehouse.silver_transform import flatten, business_columns
from fhir_lakehouse.scd2 import build_staged

PATIENT = {"resourceType": "Patient", "id": "p1", "meta": {"versionId": "1", "lastUpdated": "2026-11-09T10:00:00.000+00:00"},
           "gender": "Female", "birthDate": "1985-03-02",
           "name": [{"family": "Rao", "given": ["Asha", "K"]}],
           "telecom": [{"system": "email", "value": "a@x.com"}, {"system": "phone", "value": "123"}],
           "address": [{"line": ["1 Main"], "city": "Hyderabad", "postalCode": "500001", "country": "IN"}]}
OBS = {"resourceType": "Observation", "id": "o1", "meta": {"versionId": "1", "lastUpdated": "2026-11-09T11:00:00Z"},
       "status": "final", "code": {"coding": [{"system": "http://loinc.org", "code": "8867-4", "display": "Heart rate"}]},
       "subject": {"reference": "Patient/p1"}, "encounter": {"reference": "Encounter/e1"},
       "effectiveDateTime": "2026-11-09T10:30:00Z", "valueQuantity": {"value": 72, "unit": "/min"}}
ENC = {"resourceType": "Encounter", "id": "e1", "meta": {"versionId": "1", "lastUpdated": "2026-11-09T12:00:00Z"},
       "status": "finished", "class": {"code": "AMB", "display": "ambulatory"}, "subject": {"reference": "Patient/p1"},
       "period": {"start": "2026-11-09T10:00:00Z", "end": "2026-11-09T10:45:00Z"}}
COND = {"resourceType": "Condition", "id": "c1", "meta": {"versionId": "1", "lastUpdated": "2026-11-09T13:00:00Z"},
        "clinicalStatus": {"coding": [{"code": "active"}]}, "code": {"coding": [{"system": "s", "code": "44054006", "display": "Diabetes"}]},
        "subject": {"reference": "http://hapi/baseR4/Patient/p1"}, "onsetDateTime": "2020"}  # partial date must NOT crash (ANSI)


def bronze(spark, res):
    return spark.createDataFrame([(json.dumps(res), None, None, None, "2026-11-09", "r1", None)],
        "resource_json string, extraction_timestamp string, api_url_or_params string, source_file string, load_date string, run_id string, bronze_loaded_at string") \
        .withColumn("extraction_timestamp", F.current_timestamp()).withColumn("bronze_loaded_at", F.current_timestamp())


def test_patient_flatten(spark):
    r = flatten(bronze(spark, PATIENT), "Patient").first()
    assert (r.patient_id, r.gender, r.full_name, r.email, r.phone, r.city) == ("p1", "female", "Asha K Rao", "a@x.com", "123", "Hyderabad")
    assert str(r.birth_date) == "1985-03-02" and r.fatal_reasons == "" and len(r.row_hash) == 64


def test_other_resources_and_reference_parsing(spark):
    o = flatten(bronze(spark, OBS), "Observation").first()
    assert (o.patient_id, o.encounter_id, o.code, o.value_number, o.value_type) == ("p1", "e1", "8867-4", 72.0, "quantity")
    e = flatten(bronze(spark, ENC), "Encounter").first()
    assert e.duration_minutes == 45.0 and e.class_code == "AMB"
    c = flatten(bronze(spark, COND), "Condition").first()
    assert c.patient_id == "p1" and c.clinical_status == "active"      # absolute reference URL handled


def test_hash_changes_when_business_data_changes(spark):
    h1 = flatten(bronze(spark, PATIENT), "Patient").first().row_hash
    changed = json.loads(json.dumps(PATIENT)); changed["address"][0]["city"] = "Pune"
    h2 = flatten(bronze(spark, changed), "Patient").first().row_hash
    assert h1 != h2


def test_scd2_staging_new_changed_unchanged_and_stale(spark):
    cols = "patient_id string, row_hash string, valid_from timestamp, scd_version int, is_current boolean, valid_to timestamp"
    cur = spark.createDataFrame([("A", "h1", None, 1, True, None), ("B", "h1", None, 1, True, None),
                                 ("C", "h1", None, 1, True, None)], cols) \
        .withColumn("valid_from", F.to_timestamp(F.lit("2026-11-09 10:00:00")))
    src = spark.createDataFrame([("A", "h1", "2026-11-10 10:00:00"),    # unchanged
                                 ("B", "h2", "2026-11-10 10:00:00"),    # changed + newer
                                 ("C", "h2", "2026-11-08 10:00:00"),    # changed but OLDER -> ignore (late data)
                                 ("D", "h9", "2026-11-10 10:00:00")],   # new
                                "patient_id string, row_hash string, valid_from string") \
        .withColumn("valid_from", F.col("valid_from").cast("timestamp"))
    staged, stats = build_staged(src, cur, "patient_id")
    assert stats == {"new": 1, "changed": 1}
    rows = {(r.patient_id, r.merge_key, r.scd_version) for r in staged.collect()}
    # B appears twice: once to close the old row (merge_key=B), once to insert the new one (merge_key=None)
    assert rows == {("B", "B", 2), ("B", None, 2), ("D", "D", 1)}
