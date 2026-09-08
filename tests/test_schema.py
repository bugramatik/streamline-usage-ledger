import json
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from ledger.schema import UsageEvent, bucket_key, parse_event


def raw_event(**overrides):
    payload = {
        "event_id": "evt_7f3a91",
        "account_id": "acct_1042",
        "meter": "api_calls",
        "quantity": 12,
        "occurred_at": "2024-11-04T13:47:12Z",
        "source": "gateway",
    }
    payload.update(overrides)
    return json.dumps(payload).encode("utf-8")


def test_parse_event_ok():
    event = parse_event(raw_event())

    assert event.event_id == "evt_7f3a91"
    assert event.account_id == "acct_1042"
    assert event.meter == "api_calls"
    assert event.quantity == 12
    assert event.occurred_at == datetime(2024, 11, 4, 13, 47, 12, tzinfo=timezone.utc)
    assert event.source == "gateway"


def test_parse_event_rejects_missing_meter():
    payload = json.loads(raw_event())
    del payload["meter"]

    with pytest.raises(ValidationError):
        parse_event(json.dumps(payload).encode("utf-8"))


def test_parse_event_rejects_negative_quantity():
    with pytest.raises(ValidationError):
        parse_event(raw_event(quantity=-3))


def test_parse_event_rejects_bad_json():
    with pytest.raises(ValueError):
        parse_event(b"{not json")


def test_bucket_key_uses_hour():
    event = parse_event(raw_event(occurred_at="2024-11-04T13:59:59Z"))

    assert bucket_key(event) == ("acct_1042", "api_calls", "2024-11-04T13:00:00Z")


def test_bucket_key_separates_hours():
    early = parse_event(raw_event(occurred_at="2024-11-04T13:00:00Z"))
    late = parse_event(raw_event(occurred_at="2024-11-04T14:00:00Z"))

    assert bucket_key(early) != bucket_key(late)


def test_usage_event_is_frozen():
    event = parse_event(raw_event())

    with pytest.raises(ValidationError):
        event.quantity = 99
