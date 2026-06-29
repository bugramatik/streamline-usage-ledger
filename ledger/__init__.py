"""Streamline usage ledger: raw events in, billable and auditable usage records out."""

__version__ = "0.4.0"

# Name of the canonical event schema shared by ingest, aggregation and rating.
EVENT_SCHEMA = "streamline.usage.v1"

__all__ = ["__version__", "EVENT_SCHEMA"]
