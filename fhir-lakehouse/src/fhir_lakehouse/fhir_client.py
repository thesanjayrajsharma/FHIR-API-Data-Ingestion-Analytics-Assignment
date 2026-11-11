"""Thin, testable FHIR REST client: search + _lastUpdated window + Bundle pagination + retries."""
from __future__ import annotations
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterator, List, Optional, Tuple

import requests

RETRY_STATUS = {429, 500, 502, 503, 504}


@dataclass
class Page:
    page_no: int
    request_url: str            # exact URL called (incl. query string) -> stored as api_url_or_params
    status_code: int
    body: bytes                 # response bytes EXACTLY as received -> stored as-is in the raw layer
    called_at: datetime
    duration_ms: int
    entry_count: int
    next_url: Optional[str]
    attempts: int = 1


class FhirClient:
    def __init__(self, base_url: str, page_size: int = 100, timeout: int = 60,
                 max_retries: int = 5, backoff: float = 2.0,
                 accept: str = "application/fhir+json", session: Optional[requests.Session] = None,
                 sleep=time.sleep):
        self.base_url = base_url.rstrip("/")
        self.page_size = page_size
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff = backoff
        self.session = session or requests.Session()
        self.session.headers.update({"Accept": accept, "User-Agent": "fhir-lakehouse/1.0"})
        self._sleep = sleep

    # ------------------------------------------------------------------ search
    def search_params(self, window_start: str, window_end: str) -> List[Tuple[str, str]]:
        """_lastUpdated is a repeated parameter (ge + lt). _sort makes paging stable."""
        return [
            ("_lastUpdated", f"ge{window_start}"),
            ("_lastUpdated", f"lt{window_end}"),
            ("_sort", "_lastUpdated"),
            ("_count", str(self.page_size)),
        ]

    def iter_pages(self, resource: str, params: List[Tuple[str, str]],
                   max_pages: int = 0) -> Iterator[Page]:
        """Follow Bundle.link[rel=next] until exhausted (or max_pages for dev runs)."""
        url: Optional[str] = f"{self.base_url}/{resource}"
        first = True
        page_no = 0
        while url:
            page_no += 1
            page = self._get(url, params if first else None, page_no)
            yield page
            first = False
            if max_pages and page_no >= max_pages:
                return
            url = page.next_url

    # ------------------------------------------------------------------ http
    def _get(self, url: str, params, page_no: int) -> Page:
        attempt = 0
        while True:
            attempt += 1
            called_at = datetime.now(timezone.utc)
            t0 = time.perf_counter()
            try:
                resp = self.session.get(url, params=params, timeout=self.timeout)
            except requests.RequestException:
                if attempt > self.max_retries:
                    raise
                self._sleep(self.backoff ** attempt)
                continue
            if resp.status_code in RETRY_STATUS and attempt <= self.max_retries:
                retry_after = resp.headers.get("Retry-After")
                self._sleep(float(retry_after) if retry_after and retry_after.isdigit()
                            else self.backoff ** attempt)
                continue
            resp.raise_for_status()
            dur = int((time.perf_counter() - t0) * 1000)
            bundle = resp.json()
            entries = [e for e in bundle.get("entry", [])
                       if (e.get("search") or {}).get("mode", "match") == "match"]
            nxt = next((l["url"] for l in bundle.get("link", []) if l.get("relation") == "next"), None)
            return Page(page_no, resp.url, resp.status_code, resp.content, called_at, dur,
                        len(entries), nxt, attempt)
