# Streamline Usage Ledger

An event-driven usage metering pipeline that turns raw product events into billable, auditable usage records.

<img src="screenshots/1.png" alt="Ledger dashboard showing hourly usage buckets, the active rate card and the reconciliation summary for one day" width="800">

## Why

Metering usually ends up as a nightly batch job whose output nobody can explain when a customer disputes a line on an invoice. Late events, retried events and replayed batches turn into support tickets because the pipeline has no way to say which raw event produced which charge. This project keeps the whole path visible: raw events land on a Kafka topic, a Spark job folds them into hourly buckets, a rating service prices those buckets with versioned rate cards, and a reconciliation report compares the two sides for any day. It is for engineers who own billing pipelines and need to answer "where did this number come from" without reading logs.

## How it works

Raw JSON events are validated against one canonical schema and published to Kafka keyed by account, so every event for an account lands on the same partition. The Spark job reads that topic, drops duplicate event ids inside a one hour watermark, sums quantity per account, meter and hour, and upserts the buckets into Postgres. The Spring Boot rating service loads the rate card in force for the day, prices each bucket, and writes ledger lines idempotently. A separate reconciliation command sums both sides per account and meter and reports any difference above the tolerance.

```mermaid
flowchart LR
    src["Product surfaces"] --> ing["ledger-ingest"]
    ing --> kafka[("Kafka usage.raw")]
    kafka --> agg["Spark aggregate"]
    agg --> buckets[("usage_buckets")]
    buckets --> rate["Rating service"]
    cards[("rate_cards")] --> rate
    rate --> lines[("ledger_lines")]
    buckets --> rec["ledger-reconcile"]
    lines --> rec
    rec --> report["Reconciliation report"]
    lines --> ui["Ledger dashboard"]
```

## Key decisions

- Events are keyed by account on the Kafka topic, which keeps one account's events in order on one partition but concentrates a very large account on a single consumer task.
- Duplicates are dropped with a one hour event-time watermark rather than an unbounded deduplication table, which keeps state small but lets a duplicate that arrives later than an hour through to reconciliation.
- Ledger writes upsert on account, meter and hour and only overwrite when the incoming rate version is the same or newer, so a replay cannot undo a repriced day, at the cost of keeping old rate versions in the table.
- The rating service prices buckets in Java while aggregation runs in Python on Spark, which puts each job in the language its library fits best but means two runtimes to build and deploy.

## Tech stack

- Python 3.11 with pydantic, confluent-kafka, pyspark and psycopg
- Java 21 with Spring Boot 3.3.2, Spring JDBC and the Postgres driver
- Kafka for the raw event topic, Spark for aggregation, PostgreSQL for buckets, rate cards and ledger lines
- pytest for the Python tests, JUnit and AssertJ for the rating service

## Quick start

Prerequisites: Python 3.11 or newer, JDK 21, Maven, a Kafka broker and a PostgreSQL 15 database with the `usage_buckets`, `rate_cards` and `ledger_lines` tables.

```bash
pip install -e ".[dev]"
ledger-ingest --bootstrap localhost:9092 --topic usage.raw --file samples/events.jsonl
ledger-aggregate --bootstrap localhost:9092 --topic usage.raw --once
cd rating && mvn spring-boot:run
curl -X POST "http://localhost:8080/api/rate?day=2024-11-04"
ledger-reconcile --day 2024-11-04
pytest
```

| Variable | What it sets | Default |
| --- | --- | --- |
| `DATABASE_URL` | Postgres connection string used by the Python jobs | `postgresql://ledger:ledger@localhost:5432/ledger` |
| `KAFKA_BOOTSTRAP` | Kafka bootstrap servers for the aggregation job | `localhost:9092` |
| `KAFKA_TOPIC` | Topic the aggregation job subscribes to | `usage.events` |
| `LEDGER_JDBC_URL` | JDBC URL passed to the aggregation job | `jdbc:postgresql://localhost:5432/ledger` |
| `CHECKPOINT_DIR` | Spark streaming checkpoint directory | `/tmp/ledger-ckpt` |

## Usage

Rate one day of buckets and read the result:

```bash
$ curl -s -X POST "http://localhost:8080/api/rate?day=2024-11-04"
{"day":"2024-11-04","buckets":312,"linesWritten":312,"unpricedMeters":0,"totalAmount":184.62}
```

Reconcile the same day against the raw totals:

```bash
$ ledger-reconcile --day 2024-11-04
2024-11-04: 312/312 matched, 0 shortfall, 0 overage
```

## Project structure

```text
ledger/        ingest, aggregation, reconciliation and the shared schema
rating/        Spring Boot rating service and its tests
tests/         pytest suite for the schema and reconciliation
ui/            single-page ledger dashboard
```

## Limitations

- The deduplication window is one hour of event time, so a duplicate that arrives later is only caught by reconciliation.
- Rate cards are loaded from Postgres by hand; there is no admin API for creating a new version.
- The dashboard reads a static snapshot of one day rather than querying the rating service.
- Meters with no active rate card are skipped and counted as unpriced, not priced at zero.

## License

MIT. The project builds on Apache Kafka, Apache Spark, Spring Boot and PostgreSQL, each under its own licence.
