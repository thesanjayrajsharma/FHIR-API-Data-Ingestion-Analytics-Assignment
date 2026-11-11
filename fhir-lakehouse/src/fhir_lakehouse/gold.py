"""Silver -> Gold: star schema (dims + facts) and reporting views, built with Spark SQL templates."""
from __future__ import annotations
from .audit import StageRun

TABLES = {}   # name -> SQL (CREATE OR REPLACE TABLE ... AS SELECT)
VIEWS = {}    # name -> SQL

TABLES["dim_patient"] = """
SELECT patient_id, full_name, given_name, family_name, gender, birth_date,
       CAST(floor(months_between(current_date(), birth_date) / 12) AS INT) AS age_years,
       CASE WHEN birth_date IS NULL THEN 'Unknown'
            WHEN months_between(current_date(), birth_date)/12 < 18 THEN '0-17'
            WHEN months_between(current_date(), birth_date)/12 < 40 THEN '18-39'
            WHEN months_between(current_date(), birth_date)/12 < 65 THEN '40-64'
            ELSE '65+' END AS age_band,
       is_active, is_deceased, marital_status, language, city, state, postal_code, country, phone, email,
       scd_version AS record_version, valid_from AS last_changed_at, dq_flags
FROM {silver}.patient WHERE is_current"""

TABLES["dim_clinical_code"] = """
SELECT code_system, code, max(code_display) AS code_display, domain FROM (
  SELECT code_system, code, code_display, 'observation' AS domain FROM {silver}.observation WHERE is_current AND code IS NOT NULL
  UNION ALL
  SELECT code_system, code, code_display, 'condition' FROM {silver}.condition WHERE is_current AND code IS NOT NULL)
GROUP BY code_system, code, domain"""

TABLES["dim_date"] = """
WITH bounds AS (
  SELECT least(coalesce(min(CAST(period_start AS DATE)), current_date()), date'2020-01-01') AS d0, current_date() AS d1
  FROM {silver}.encounter WHERE is_current),
days AS (SELECT explode(sequence(d0, d1, interval 1 day)) AS d FROM bounds)
SELECT d AS date_key, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
       date_format(d, 'MMM') AS month_name, day(d) AS day, date_format(d, 'EEEE') AS weekday,
       weekofyear(d) AS week_of_year, dayofweek(d) IN (1,7) AS is_weekend
FROM days"""

TABLES["fact_encounter"] = """
SELECT e.encounter_id, e.patient_id, CAST(e.period_start AS DATE) AS date_key, e.status, e.class_code, e.class_display,
       e.type_code, e.type_display, e.period_start, e.period_end, e.duration_minutes, e.reason_code, e.reason_display,
       e.practitioner_id, e.service_provider_id, p.patient_id IS NULL AS is_orphan_patient, e.valid_from AS last_changed_at
FROM {silver}.encounter e LEFT JOIN {gold}.dim_patient p ON e.patient_id = p.patient_id WHERE e.is_current"""

TABLES["fact_observation"] = """
SELECT o.observation_id, o.patient_id, o.encounter_id, CAST(o.effective_ts AS DATE) AS date_key, o.status, o.category_code,
       o.code_system, o.code, o.code_display, o.effective_ts, o.value_number, o.value_unit, o.value_text, o.value_boolean,
       o.value_type, o.interpretation_code, p.patient_id IS NULL AS is_orphan_patient, o.valid_from AS last_changed_at
FROM {silver}.observation o LEFT JOIN {gold}.dim_patient p ON o.patient_id = p.patient_id WHERE o.is_current"""

TABLES["fact_condition"] = """
SELECT c.condition_id, c.patient_id, c.encounter_id, CAST(coalesce(c.onset_ts, c.recorded_date) AS DATE) AS date_key,
       c.clinical_status, c.verification_status, c.category_code, c.severity, c.code_system, c.code, c.code_display,
       c.onset_ts, c.abatement_ts, c.recorded_date, c.abatement_ts IS NULL AND coalesce(c.clinical_status,'active')='active' AS is_active_now,
       p.patient_id IS NULL AS is_orphan_patient, c.valid_from AS last_changed_at
FROM {silver}.condition c LEFT JOIN {gold}.dim_patient p ON c.patient_id = p.patient_id WHERE c.is_current"""

VIEWS["vw_patient_360"] = """
SELECT p.*, coalesce(e.encounters,0) AS encounters, coalesce(o.observations,0) AS observations,
       coalesce(c.conditions,0) AS conditions, coalesce(c.active_conditions,0) AS active_conditions, e.last_encounter
FROM {gold}.dim_patient p
LEFT JOIN (SELECT patient_id, count(*) encounters, max(period_start) last_encounter FROM {gold}.fact_encounter GROUP BY 1) e USING (patient_id)
LEFT JOIN (SELECT patient_id, count(*) observations FROM {gold}.fact_observation GROUP BY 1) o USING (patient_id)
LEFT JOIN (SELECT patient_id, count(*) conditions, sum(CAST(is_active_now AS INT)) active_conditions FROM {gold}.fact_condition GROUP BY 1) c USING (patient_id)"""

VIEWS["vw_encounters_daily"] = """
SELECT date_key, class_display, count(*) AS encounters, round(avg(duration_minutes),1) AS avg_duration_min,
       count(DISTINCT patient_id) AS patients
FROM {gold}.fact_encounter GROUP BY date_key, class_display"""

VIEWS["vw_top_conditions"] = """
SELECT code, code_display, count(*) AS cases, count(DISTINCT patient_id) AS patients,
       rank() OVER (ORDER BY count(*) DESC) AS rnk
FROM {gold}.fact_condition WHERE code IS NOT NULL GROUP BY code, code_display"""

VIEWS["vw_vitals"] = """
SELECT patient_id, date_key, effective_ts,
       CASE code WHEN '8867-4' THEN 'Heart rate' WHEN '8480-6' THEN 'Systolic BP' WHEN '8462-4' THEN 'Diastolic BP'
                 WHEN '29463-7' THEN 'Body weight' WHEN '8302-2' THEN 'Body height' WHEN '39156-5' THEN 'BMI'
                 WHEN '8310-5' THEN 'Body temperature' WHEN '9279-1' THEN 'Respiratory rate' END AS vital,
       value_number, value_unit
FROM {gold}.fact_observation
WHERE code IN ('8867-4','8480-6','8462-4','29463-7','8302-2','39156-5','8310-5','9279-1') AND value_number IS NOT NULL"""

VIEWS["vw_patient_history"] = """
SELECT patient_id, scd_version, valid_from, valid_to, is_current, full_name, gender, birth_date, city, state, postal_code, phone, email
FROM {silver}.patient"""

VIEWS["vw_scd2_change_audit"] = """
SELECT 'Patient' AS resource, patient_id AS resource_id, max(scd_version) AS versions, min(valid_from) AS first_seen, max(valid_from) AS last_changed
FROM {silver}.patient GROUP BY patient_id HAVING max(scd_version) > 1
UNION ALL SELECT 'Encounter', encounter_id, max(scd_version), min(valid_from), max(valid_from) FROM {silver}.encounter GROUP BY encounter_id HAVING max(scd_version) > 1
UNION ALL SELECT 'Observation', observation_id, max(scd_version), min(valid_from), max(valid_from) FROM {silver}.observation GROUP BY observation_id HAVING max(scd_version) > 1
UNION ALL SELECT 'Condition', condition_id, max(scd_version), min(valid_from), max(valid_from) FROM {silver}.condition GROUP BY condition_id HAVING max(scd_version) > 1"""

VIEWS["vw_pipeline_health"] = """
SELECT r.run_id, r.stage, r.resource, r.status, r.rows_in, r.rows_out, r.details,
       r.started_at, r.ended_at, round(unix_timestamp(r.ended_at) - unix_timestamp(r.started_at), 1) AS seconds, r.error
FROM {ctrl}.pipeline_run_log r"""

VIEWS["vw_api_call_audit"] = """
SELECT resource, load_date, count(*) AS pages, sum(entry_count) AS entries, sum(bytes) AS bytes,
       min(called_at) AS first_call, max(called_at) AS last_call, max(saved_at) AS last_saved,
       sum(attempts - 1) AS retries, round(avg(duration_ms)) AS avg_ms
FROM {ctrl}.api_call_log GROUP BY resource, load_date"""

VIEWS["vw_data_quality"] = """
SELECT 'Patient' AS resource, count(*) AS current_rows, sum(CAST(dq_flags != '' AS INT)) AS flagged FROM {silver}.patient WHERE is_current
UNION ALL SELECT 'Encounter', count(*), sum(CAST(dq_flags != '' AS INT)) FROM {silver}.encounter WHERE is_current
UNION ALL SELECT 'Observation', count(*), sum(CAST(dq_flags != '' AS INT)) FROM {silver}.observation WHERE is_current
UNION ALL SELECT 'Condition', count(*), sum(CAST(dq_flags != '' AS INT)) FROM {silver}.condition WHERE is_current
UNION ALL SELECT 'Quarantined (all runs)', count(*), count(*) FROM {silver}.dq_quarantine"""


def build_gold(spark, cfg, run_id: str):
    fmt = dict(silver=cfg.schema("silver"), gold=cfg.schema("gold"), ctrl=cfg.schema("ctrl"))
    with StageRun(spark, cfg, run_id, "gold", None) as st:
        for name, sql in TABLES.items():      # dict preserves order: dim_patient before facts
            spark.sql(f"CREATE OR REPLACE TABLE {fmt['gold']}.{name} AS {sql.format(**fmt)}")
        for name, sql in VIEWS.items():
            spark.sql(f"CREATE OR REPLACE VIEW {fmt['gold']}.{name} AS {sql.format(**fmt)}")
        st.set(rows_out=len(TABLES) + len(VIEWS), details=f"{len(TABLES)} tables, {len(VIEWS)} views")
