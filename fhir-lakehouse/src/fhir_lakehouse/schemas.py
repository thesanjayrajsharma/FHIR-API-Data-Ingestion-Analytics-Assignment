"""DDL schemas used with from_json for the four FHIR R4 resources (only the fields we model)."""

_CODING = "array<struct<system:string,code:string,display:string>>"
_CC = f"struct<text:string,coding:{_CODING}>"
_META = "struct<versionId:string,lastUpdated:string>"
_REF = "struct<reference:string>"

PATIENT = f"""id string, meta {_META}, active boolean, gender string, birthDate string,
  deceasedBoolean boolean, deceasedDateTime string,
  name array<struct<use:string,family:string,given:array<string>>>,
  telecom array<struct<system:string,value:string,use:string>>,
  address array<struct<use:string,line:array<string>,city:string,state:string,postalCode:string,country:string>>,
  maritalStatus {_CC},
  communication array<struct<language:{_CC}>>,
  managingOrganization {_REF}"""

ENCOUNTER = f"""id string, meta {_META}, status string,
  class struct<system:string,code:string,display:string>,
  type array<{_CC}>, subject {_REF},
  participant array<struct<individual:{_REF}>>,
  period struct<start:string,end:string>,
  reasonCode array<{_CC}>, serviceProvider {_REF}"""

OBSERVATION = f"""id string, meta {_META}, status string,
  category array<{_CC}>, code {_CC}, subject {_REF}, encounter {_REF},
  effectiveDateTime string, effectivePeriod struct<start:string,end:string>, issued string,
  valueQuantity struct<value:double,unit:string,system:string,code:string>,
  valueString string, valueBoolean boolean, valueInteger long, valueCodeableConcept {_CC},
  interpretation array<{_CC}>"""

CONDITION = f"""id string, meta {_META},
  clinicalStatus {_CC}, verificationStatus {_CC}, category array<{_CC}>, severity {_CC},
  code {_CC}, subject {_REF}, encounter {_REF},
  onsetDateTime string, abatementDateTime string, recordedDate string"""

SCHEMAS = {"Patient": PATIENT, "Encounter": ENCOUNTER, "Observation": OBSERVATION, "Condition": CONDITION}
