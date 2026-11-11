"""Pure DataFrame transforms: parse JSON -> typed, flattened, hashed rows. No I/O, fully unit-testable."""
from __future__ import annotations
from typing import Dict, List

from pyspark.sql import Column, DataFrame, functions as F

from .schemas import SCHEMAS

TS = lambda c: F.expr(f"try_cast({c} as timestamp)")
DT = lambda c: F.expr(f"try_cast({c} as date)")
REF_ID = lambda col, typ: F.expr(f"nullif(regexp_extract({col}, '{typ}/([^/?#]+)', 1), '')")


def _first_coding(cc: str, field: str) -> Column:
    return F.expr(f"{cc}.coding[0].{field}")


def _cc_text(cc: str) -> Column:
    return F.expr(f"coalesce({cc}.coding[0].display, {cc}.text)")


def _patient(p: str) -> Dict[str, Column]:
    return {
        "patient_id": F.col(f"{p}.id"),
        "is_active": F.col(f"{p}.active"),
        "gender": F.lower(F.col(f"{p}.gender")),
        "birth_date": DT(f"{p}.birthDate"),
        "is_deceased": F.expr(f"coalesce({p}.deceasedBoolean, {p}.deceasedDateTime is not null)"),
        "family_name": F.expr(f"{p}.name[0].family"),
        "given_name": F.expr(f"{p}.name[0].given[0]"),
        "full_name": F.expr(f"nullif(trim(concat_ws(' ', array_join({p}.name[0].given, ' '), {p}.name[0].family)), '')"),
        "phone": F.expr(f"filter({p}.telecom, x -> x.system = 'phone')[0].value"),
        "email": F.expr(f"filter({p}.telecom, x -> x.system = 'email')[0].value"),
        "address_line": F.expr(f"{p}.address[0].line[0]"),
        "city": F.expr(f"{p}.address[0].city"),
        "state": F.expr(f"{p}.address[0].state"),
        "postal_code": F.expr(f"{p}.address[0].postalCode"),
        "country": F.expr(f"{p}.address[0].country"),
        "marital_status": F.expr(f"coalesce({p}.maritalStatus.coding[0].code, {p}.maritalStatus.text)"),
        "language": F.expr(f"{p}.communication[0].language.coding[0].code"),
        "managing_org_id": REF_ID(f"{p}.managingOrganization.reference", "Organization"),
    }


def _encounter(p: str) -> Dict[str, Column]:
    return {
        "encounter_id": F.col(f"{p}.id"),
        "patient_id": REF_ID(f"{p}.subject.reference", "Patient"),
        "status": F.col(f"{p}.status"),
        "class_code": F.expr(f"{p}.class.code"),
        "class_display": F.expr(f"{p}.class.display"),
        "type_code": F.expr(f"{p}.type[0].coding[0].code"),
        "type_display": _cc_text(f"{p}.type[0]"),
        "period_start": TS(f"{p}.period.start"),
        "period_end": TS(f"{p}.period.end"),
        "duration_minutes": F.expr(
            f"CASE WHEN try_cast({p}.period.end as timestamp) IS NOT NULL AND try_cast({p}.period.start as timestamp) IS NOT NULL "
            f"THEN (unix_timestamp(try_cast({p}.period.end as timestamp)) - unix_timestamp(try_cast({p}.period.start as timestamp)))/60.0 END"),
        "practitioner_id": REF_ID(f"{p}.participant[0].individual.reference", "Practitioner"),
        "reason_code": F.expr(f"{p}.reasonCode[0].coding[0].code"),
        "reason_display": _cc_text(f"{p}.reasonCode[0]"),
        "service_provider_id": REF_ID(f"{p}.serviceProvider.reference", "Organization"),
    }


def _observation(p: str) -> Dict[str, Column]:
    return {
        "observation_id": F.col(f"{p}.id"),
        "patient_id": REF_ID(f"{p}.subject.reference", "Patient"),
        "encounter_id": REF_ID(f"{p}.encounter.reference", "Encounter"),
        "status": F.col(f"{p}.status"),
        "category_code": F.expr(f"{p}.category[0].coding[0].code"),
        "code_system": F.expr(f"{p}.code.coding[0].system"),
        "code": F.expr(f"{p}.code.coding[0].code"),
        "code_display": _cc_text(f"{p}.code"),
        "effective_ts": F.expr(f"coalesce(try_cast({p}.effectiveDateTime as timestamp), try_cast({p}.effectivePeriod.start as timestamp))"),
        "issued_ts": TS(f"{p}.issued"),
        "value_number": F.expr(f"coalesce({p}.valueQuantity.value, cast({p}.valueInteger as double))"),
        "value_unit": F.expr(f"{p}.valueQuantity.unit"),
        "value_text": F.expr(f"coalesce({p}.valueString, {p}.valueCodeableConcept.text, {p}.valueCodeableConcept.coding[0].display)"),
        "value_boolean": F.col(f"{p}.valueBoolean"),
        "value_type": F.expr(
            f"CASE WHEN {p}.valueQuantity IS NOT NULL THEN 'quantity' WHEN {p}.valueInteger IS NOT NULL THEN 'integer' "
            f"WHEN {p}.valueString IS NOT NULL THEN 'string' WHEN {p}.valueCodeableConcept IS NOT NULL THEN 'codeable' "
            f"WHEN {p}.valueBoolean IS NOT NULL THEN 'boolean' ELSE 'none' END"),
        "interpretation_code": F.expr(f"{p}.interpretation[0].coding[0].code"),
    }


def _condition(p: str) -> Dict[str, Column]:
    return {
        "condition_id": F.col(f"{p}.id"),
        "patient_id": REF_ID(f"{p}.subject.reference", "Patient"),
        "encounter_id": REF_ID(f"{p}.encounter.reference", "Encounter"),
        "clinical_status": F.expr(f"{p}.clinicalStatus.coding[0].code"),
        "verification_status": F.expr(f"{p}.verificationStatus.coding[0].code"),
        "category_code": F.expr(f"{p}.category[0].coding[0].code"),
        "severity": _cc_text(f"{p}.severity"),
        "code_system": F.expr(f"{p}.code.coding[0].system"),
        "code": F.expr(f"{p}.code.coding[0].code"),
        "code_display": _cc_text(f"{p}.code"),
        "onset_ts": TS(f"{p}.onsetDateTime"),
        "abatement_ts": TS(f"{p}.abatementDateTime"),
        "recorded_date": DT(f"{p}.recordedDate"),
    }


_BUILDERS = {"Patient": _patient, "Encounter": _encounter, "Observation": _observation, "Condition": _condition}
BUSINESS_KEY = {"Patient": "patient_id", "Encounter": "encounter_id",
                "Observation": "observation_id", "Condition": "condition_id"}

# DQ rules: (flag name, SQL predicate that is TRUE when the row is *bad*). 'fatal' rows go to quarantine.
DQ_RULES = {
    "Patient": [("future_birth_date", "birth_date > current_date()", False)],
    "Encounter": [("missing_patient_ref", "patient_id IS NULL", False),
                  ("period_end_before_start", "period_end < period_start", False)],
    "Observation": [("missing_patient_ref", "patient_id IS NULL", False),
                    ("missing_code", "code IS NULL AND code_display IS NULL", False)],
    "Condition": [("missing_patient_ref", "patient_id IS NULL", False),
                  ("missing_code", "code IS NULL AND code_display IS NULL", False)],
}
FATAL = [("missing_id", "{key} IS NULL"), ("missing_last_updated", "valid_from IS NULL")]


def business_columns(resource: str) -> List[str]:
    return list(_BUILDERS[resource]("p").keys())


def flatten(bronze: DataFrame, resource: str) -> DataFrame:
    """bronze (resource_json, ...) -> typed columns + row_hash + dq_flags + lineage."""
    key = BUSINESS_KEY[resource]
    cols = _BUILDERS[resource]("p")
    parsed = bronze.withColumn("p", F.from_json("resource_json", SCHEMAS[resource]))
    df = parsed.select(
        *[v.alias(k) for k, v in cols.items()],
        F.col("p.meta.versionId").alias("source_version_id"),
        TS("p.meta.lastUpdated").alias("valid_from"),
        "resource_json", "extraction_timestamp", "api_url_or_params", "source_file",
        "load_date", "run_id", "bronze_loaded_at")
    # change-detection hash over business attributes only (NOT lineage / timestamps of ingestion)
    hash_cols = [F.coalesce(F.col(c).cast("string"), F.lit("\u2205")) for c in cols]
    df = df.withColumn("row_hash", F.sha2(F.concat_ws("\u0001", *hash_cols), 256))
    flags = [F.when(F.expr(pred), F.lit(name)) for name, pred, _ in DQ_RULES[resource]]
    df = df.withColumn("dq_flags", F.array_join(F.filter(F.array(*flags), lambda x: x.isNotNull()), ","))
    reasons = [F.when(F.expr(p.format(key=key)), F.lit(n)) for n, p in FATAL]
    return df.withColumn("fatal_reasons", F.array_join(F.filter(F.array(*reasons), lambda x: x.isNotNull()), ","))
