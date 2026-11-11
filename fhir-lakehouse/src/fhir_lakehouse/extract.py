"""API -> Raw layer. Every page is saved byte-for-byte; every call is logged."""
from __future__ import annotations
import json
import os
from dataclasses import dataclass
from typing import List

from .util import utcnow
from .fhir_client import FhirClient
from .windows import Window


def raw_dir(cfg, resource: str, load_date: str) -> str:
    return f"{cfg.raw_root}/{resource.lower()}/load_date={load_date}"


def raw_file_name(run_id: str, resource: str, load_date: str, page_no: int, ext: str = "json") -> str:
    return f"{run_id}__{resource.lower()}__{load_date}__p{page_no:05d}.{ext}"


@dataclass
class ExtractResult:
    pages: int
    entries: int
    call_rows: List[dict]


def extract_resource(client: FhirClient, cfg, resource: str, window: Window, run_id: str,
                     raw_root_override: str | None = None) -> ExtractResult:
    """Pull every page for `resource` in `window` and persist untouched response bodies."""
    out_dir = (f"{raw_root_override}/{resource.lower()}/load_date={window.load_date}"
               if raw_root_override else raw_dir(cfg, resource, window.load_date))
    os.makedirs(out_dir, exist_ok=True)
    params = client.search_params(window.start_s, window.end_s)
    rows, entries = [], 0
    for page in client.iter_pages(resource, params, max_pages=int(cfg.fhir.get("max_pages_per_run", 0))):
        fname = raw_file_name(run_id, resource, window.load_date, page.page_no)
        with open(os.path.join(out_dir, fname), "wb") as fh:
            fh.write(page.body)                                   # as-is
        rows.append(dict(
            run_id=run_id, resource=resource, load_date=window.load_date,
            window_start=window.start_s, window_end=window.end_s, page_no=page.page_no,
            request_url=page.request_url, http_status=page.status_code, attempts=page.attempts,
            entry_count=page.entry_count, bytes=len(page.body), duration_ms=page.duration_ms,
            called_at=page.called_at.replace(tzinfo=None), raw_file_name=fname, saved_at=utcnow()))
        entries += page.entry_count
    # sidecar so the same run can be replayed from the landing zone (offline/air-gapped fallback)
    with open(os.path.join(out_dir, f"_calls__{run_id}__{resource.lower()}__{window.load_date}.jsonl"), "w") as fh:
        for r in rows:
            fh.write(json.dumps(r, default=str) + "\n")
    return ExtractResult(len(rows), entries, rows)
