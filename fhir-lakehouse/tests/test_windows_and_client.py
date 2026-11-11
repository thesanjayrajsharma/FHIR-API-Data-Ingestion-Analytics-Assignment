import json
from datetime import date, datetime, timezone
from fhir_lakehouse.windows import compute_window
from fhir_lakehouse.fhir_client import FhirClient


def test_daily_window_is_exact_24h():
    w = compute_window("daily", date(2026, 11, 9))
    assert (w.start_s, w.end_s, w.load_date) == ("2026-11-09T00:00:00Z", "2026-11-10T00:00:00Z", "2026-11-09")


def test_incremental_uses_watermark_and_falls_back_first_run():
    now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
    wm = datetime(2026, 11, 10, 8, 30, tzinfo=timezone.utc)
    assert compute_window("incremental", date(2026, 11, 11), wm, now).start_s == "2026-11-10T08:30:00Z"
    assert compute_window("incremental", date(2026, 11, 11), None, now).start_s == "2026-11-11T00:00:00Z"


class FakeResp:
    def __init__(self, payload, status=200, url="http://x"):
        self._p, self.status_code, self.url = payload, status, url
        self.content = json.dumps(payload).encode()
        self.headers = {}
    def json(self): return self._p
    def raise_for_status(self):
        if self.status_code >= 400: raise RuntimeError(self.status_code)


class FakeSession:
    def __init__(self, responses): self.responses, self.calls, self.headers = list(responses), [], {}
    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params)); return self.responses.pop(0)


def bundle(n, nxt=None):
    b = {"resourceType": "Bundle", "entry": [{"resource": {"resourceType": "Patient", "id": str(i)}, "search": {"mode": "match"}} for i in range(n)],
         "link": [{"relation": "self", "url": "s"}]}
    if nxt: b["link"].append({"relation": "next", "url": nxt})
    return b


def test_pagination_follows_next_links_and_keeps_bytes():
    s = FakeSession([FakeResp(bundle(2, "http://p2")), FakeResp(bundle(2, "http://p3")), FakeResp(bundle(1))])
    c = FhirClient("http://base", session=s, sleep=lambda _: None)
    pages = list(c.iter_pages("Patient", c.search_params("a", "b")))
    assert [p.entry_count for p in pages] == [2, 2, 1]
    assert s.calls[0][1] is not None and s.calls[1] == ("http://p2", None)   # params only on first call
    assert pages[0].body == json.dumps(bundle(2, "http://p2")).encode()     # untouched bytes


def test_retry_on_503_then_success():
    s = FakeSession([FakeResp({}, 503), FakeResp(bundle(1))])
    c = FhirClient("http://base", session=s, sleep=lambda _: None)
    p = list(c.iter_pages("Patient", []))[0]
    assert p.attempts == 2 and p.entry_count == 1


def test_max_pages_cap():
    s = FakeSession([FakeResp(bundle(1, "n1")), FakeResp(bundle(1, "n2")), FakeResp(bundle(1))])
    c = FhirClient("http://base", session=s, sleep=lambda _: None)
    assert len(list(c.iter_pages("Patient", [], max_pages=2))) == 2
