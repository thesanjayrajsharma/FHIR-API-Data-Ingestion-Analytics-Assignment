"""Incremental window logic: which slice of _lastUpdated do we ask the API for?"""
from __future__ import annotations
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Optional

ISO = "%Y-%m-%dT%H:%M:%SZ"


@dataclass(frozen=True)
class Window:
    start: datetime          # inclusive (ge)
    end: datetime            # exclusive (lt)
    load_date: str           # date bucket used for the raw folder, YYYY-MM-DD
    mode: str

    @property
    def start_s(self) -> str:
        return self.start.strftime(ISO)

    @property
    def end_s(self) -> str:
        return self.end.strftime(ISO)


def _day_start(d: date) -> datetime:
    return datetime(d.year, d.month, d.day, tzinfo=timezone.utc)


def compute_window(mode: str, load_date: date, watermark: Optional[datetime] = None,
                   now: Optional[datetime] = None) -> Window:
    """
    mode = 'daily'       -> exactly [load_date, load_date+1d). Deterministic, idempotent, ideal for backfills.
    mode = 'incremental' -> (watermark, now). Picks up wherever the last successful run stopped;
                            falls back to start-of-load_date on the very first run.
    """
    if mode == "daily":
        s = _day_start(load_date)
        return Window(s, s + timedelta(days=1), load_date.isoformat(), mode)
    if mode == "incremental":
        now = now or datetime.now(timezone.utc)
        s = watermark.astimezone(timezone.utc) if watermark else _day_start(load_date)
        return Window(s, now, load_date.isoformat(), mode)
    raise ValueError(f"Unknown mode {mode!r} (expected 'daily' or 'incremental')")
