from datetime import datetime, timezone


def utcnow() -> datetime:
    """Naive UTC timestamp (Spark TimestampType friendly)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)
