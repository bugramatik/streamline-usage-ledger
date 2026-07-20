"""Canonical usage event model shared by the ingest and aggregation jobs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

EVENT_SCHEMA = "streamline.usage.v1"


class UsageEvent(BaseModel):
    """One billable unit reported by a product surface."""

    model_config = ConfigDict(frozen=True)

    event_id: str = Field(min_length=8, max_length=64)
    account_id: str = Field(min_length=1, max_length=64)
    meter: str = Field(min_length=1, max_length=64)
    quantity: Decimal = Field(gt=0)
    occurred_at: datetime
    source: str = Field(default="api", max_length=32)

    @field_validator("occurred_at")
    @classmethod
    def _require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @field_validator("quantity")
    @classmethod
    def _limit_precision(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.0001"))


class EventError(ValueError):
    """Raised when a raw record cannot be read as a usage event."""


def parse_event(raw: bytes | str) -> UsageEvent:
    """Parses one JSON record into a UsageEvent, raising EventError when it is invalid."""
    try:
        payload: Any = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise EventError(f"record is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise EventError("record must be a JSON object")
    try:
        return UsageEvent.model_validate(payload)
    except (ValueError, InvalidOperation) as exc:
        raise EventError(f"record does not match {EVENT_SCHEMA}: {exc}") from exc


def bucket_key(event: UsageEvent) -> tuple[str, str, str]:
    """Returns (account_id, meter, hour) with the hour as an ISO 8601 UTC string."""
    hour = event.occurred_at.replace(minute=0, second=0, microsecond=0)
    return event.account_id, event.meter, hour.strftime("%Y-%m-%dT%H:00:00Z")
