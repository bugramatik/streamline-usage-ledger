"""Spark job that turns raw usage events into hourly usage buckets.

Reads the Kafka topic written by ``ledger.ingest``, drops duplicate event ids
inside a watermark window, aggregates quantity per account, meter and hour and
upserts the result into Postgres. The upsert is keyed by (account, meter, hour)
so a replayed batch overwrites the same row instead of adding a second one.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ledger import EVENT_SCHEMA
from ledger.db import connect, upsert_buckets

EVENT_STRUCT = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("meter", StringType(), False),
        StructField("quantity", DoubleType(), False),
        StructField("occurred_at", TimestampType(), False),
        StructField("source", StringType(), True),
    ]
)

DEFAULT_BOOTSTRAP = "localhost:9092"
DEFAULT_TOPIC = "usage.events"
DEFAULT_JDBC = "jdbc:postgresql://localhost:5432/ledger"


def build_spark(app_name: str = "streamline-aggregate") -> SparkSession:
    """Create the Spark session with the Kafka and Postgres connectors on the classpath."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def read_events(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    """Read the raw event topic as a stream of parsed rows.

    The Kafka value is a JSON document matching ``ledger.schema.UsageEvent``.
    Rows that do not parse are dropped rather than failing the whole batch; the
    ingest step already validated them, so a bad row here means a producer
    outside this pipeline wrote to the topic.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_STRUCT).alias("event")
    ).select("event.*")
    return parsed.where(F.col("event_id").isNotNull() & F.col("meter").isNotNull())


def aggregate(df: DataFrame) -> DataFrame:
    """Deduplicate by event id and sum quantity into hourly buckets.

    The deduplication window is one hour of event time, which covers the retry
    window of the producers that feed the topic. A duplicate that arrives later
    than that is caught by the reconciliation report instead.
    """
    deduped = df.withWatermark("occurred_at", "1 hour").dropDuplicates(
        ["event_id", "occurred_at"]
    )
    bucketed = deduped.withColumn("hour", F.date_trunc("hour", F.col("occurred_at")))
    return bucketed.groupBy("account_id", "meter", "hour").agg(
        F.sum("quantity").alias("quantity"),
        F.count("event_id").alias("event_count"),
        F.max("occurred_at").alias("last_event_at"),
    )


def write_buckets(df: DataFrame, jdbc_url: str) -> None:
    """Write each micro-batch of buckets to Postgres with an idempotent upsert."""

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        rows = [
            {
                "account_id": row.account_id,
                "meter": row.meter,
                "hour": row.hour,
                "quantity": float(row.quantity),
                "event_count": int(row.event_count),
                "last_event_at": row.last_event_at,
            }
            for row in batch_df.collect()
        ]
        if not rows:
            return
        with connect() as conn:
            written = upsert_buckets(conn, rows)
        print(f"batch {batch_id}: upserted {written} buckets", file=sys.stderr)

    (
        df.writeStream.foreachBatch(_write)
        .outputMode("update")
        .option("checkpointLocation", os.environ.get("CHECKPOINT_DIR", "/tmp/ledger-ckpt"))
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Aggregate usage events into hourly buckets")
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", DEFAULT_BOOTSTRAP))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--jdbc-url", default=os.environ.get("LEDGER_JDBC_URL", DEFAULT_JDBC))
    parser.add_argument("--once", action="store_true", help="process available data and exit")
    args = parser.parse_args(argv)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    events = read_events(spark, args.bootstrap, args.topic)
    buckets = aggregate(events)

    if args.once:
        rows = buckets.collect()
        payload = [
            {
                "account_id": r.account_id,
                "meter": r.meter,
                "hour": r.hour,
                "quantity": float(r.quantity),
                "event_count": int(r.event_count),
                "last_event_at": r.last_event_at,
            }
            for r in rows
        ]
        with connect() as conn:
            written = upsert_buckets(conn, payload)
        print(f"{EVENT_SCHEMA}: upserted {written} buckets at {datetime.now(timezone.utc).isoformat()}")
        spark.stop()
        return 0

    write_buckets(buckets, args.jdbc_url)
    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
