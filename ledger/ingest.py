"""Read raw usage events, validate them and publish them to Kafka.

Events are keyed by account so every event for one account lands on the same
partition and the aggregation job sees them in order. Malformed lines are
counted and skipped rather than stopping the run, because a single bad line
from a client should not hold up a day of metering.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable, Iterator

from confluent_kafka import Producer

from ledger.schema import EventError, UsageEvent, parse_event

DEFAULT_TOPIC = "usage.raw"


def build_producer(bootstrap: str) -> Producer:
    """Create a producer that waits for the broker to acknowledge each message."""
    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "retries": 5,
            "linger.ms": 20,
        }
    )


def read_lines(path: str | None) -> Iterator[bytes]:
    """Yield raw lines from a file, or from stdin when no path is given."""
    if path is None or path == "-":
        for line in sys.stdin.buffer:
            yield line
        return
    with Path(path).open("rb") as handle:
        for line in handle:
            yield line


def publish(producer: Producer, topic: str, events: Iterable[UsageEvent]) -> int:
    """Send each event keyed by account and return how many were acknowledged."""
    sent = 0
    for event in events:
        delivered: list[str] = []

        def _on_delivery(err, _msg, event_id: str = event.event_id, out: list[str] = delivered) -> None:
            if err is not None:
                out.append(str(err))

        producer.produce(
            topic,
            key=event.account_id.encode("utf-8"),
            value=event.model_dump_json().encode("utf-8"),
            on_delivery=_on_delivery,
        )
        producer.poll(0)
        producer.flush(timeout=10)
        if delivered:
            print(f"failed to publish {event.event_id}: {delivered[0]}", file=sys.stderr)
            continue
        sent += 1
    producer.flush(timeout=10)
    return sent


def parse_lines(lines: Iterable[bytes]) -> tuple[list[UsageEvent], int]:
    """Parse raw lines, returning the valid events and the count of bad ones."""
    events: list[UsageEvent] = []
    rejected = 0
    for number, line in enumerate(lines, start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            events.append(parse_event(stripped))
        except EventError as exc:
            rejected += 1
            print(f"line {number}: {exc}", file=sys.stderr)
    return events, rejected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Publish raw usage events to Kafka.")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="target topic")
    parser.add_argument("--file", default=None, help="JSON lines file, stdin when omitted")
    parser.add_argument("--dry-run", action="store_true", help="validate only, publish nothing")
    args = parser.parse_args(argv)

    events, rejected = parse_lines(read_lines(args.file))
    if args.dry_run:
        print(f"validated {len(events)} events, {rejected} rejected", file=sys.stderr)
        return 0 if rejected == 0 else 1

    producer = build_producer(args.bootstrap)
    try:
        sent = publish(producer, args.topic, events)
    finally:
        producer.flush(timeout=10)

    print(f"published {sent} events to {args.topic}, {rejected} rejected", file=sys.stderr)
    return 0 if sent == len(events) else 1


if __name__ == "__main__":
    raise SystemExit(main())
