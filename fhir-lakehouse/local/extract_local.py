"""
Offline / air-gapped fallback: run the SAME extractor on your laptop, then upload ./landing to the Volume.

    python local/extract_local.py --from-date 2026-11-08 --days 3 --run-id bf-2026-11-11 --max-pages 25
    databricks fs cp -r ./landing dbfs:/Volumes/workspace/fhir_raw/landing/

Then run notebook 01_ingest with source=landing and the same run_id / load_date.
Needs only: requests, pyyaml (no Spark).
"""
import argparse, os, sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from fhir_lakehouse.config import load_config            # noqa: E402
from fhir_lakehouse.extract import extract_resource      # noqa: E402
from fhir_lakehouse.fhir_client import FhirClient        # noqa: E402
from fhir_lakehouse.windows import compute_window        # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--from-date", required=True, help="first load date, YYYY-MM-DD")
ap.add_argument("--days", type=int, default=3)
ap.add_argument("--run-id", required=True)
ap.add_argument("--out", default="./landing")
ap.add_argument("--max-pages", type=int, default=None, help="override fhir.max_pages_per_run")
a = ap.parse_args()

cfg = load_config(overrides={"fhir.max_pages_per_run": a.max_pages} if a.max_pages is not None else None)
f = cfg.fhir
client = FhirClient(f["base_url"], f["page_size"], f["timeout_seconds"], f["max_retries"], f["backoff_seconds"])
start = date.fromisoformat(a.from_date)
for i in range(a.days):
    d = start + timedelta(days=i)
    for res in cfg.resources:                                   # Patient -> Encounter -> Observation -> Condition
        r = extract_resource(client, cfg, res, compute_window("daily", d), a.run_id, raw_root_override=a.out)
        print(f"{d} {res:<12} pages={r.pages:<4} entries={r.entries}")
