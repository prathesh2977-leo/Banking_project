"""Stream landed banking transactions into Delta analytics tables."""

import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    max as spark_max,
    sum as spark_sum,
    upper,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType()),
        StructField("account_id", StringType()),
        StructField("card_id", StringType()),
        StructField("transaction_ts", TimestampType()),
        StructField("amount", DoubleType()),
        StructField("currency", StringType()),
        StructField("merchant_id", StringType()),
        StructField("merchant_category", StringType()),
        StructField("channel", StringType()),
        StructField("location_country", StringType()),
        StructField("event_type", StringType()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--checkpoint-path", required=True)
    parser.add_argument("--catalog", default="main")
    parser.add_argument("--schema", default="banking_analytics")
    parser.add_argument("--velocity-threshold", type=int, default=5)
    parser.add_argument("--amount-threshold", type=float, default=10000.0)
    parser.add_argument("--max-files-per-trigger", type=int, default=1000)
    args = parser.parse_args()

    for name, value in (("catalog", args.catalog), ("schema", args.schema)):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            parser.error(f"--{name} must be a valid Unity Catalog identifier")
    if args.velocity_threshold < 1 or args.amount_threshold <= 0:
        parser.error("fraud thresholds must be positive")
    if args.max_files_per_trigger < 1:
        parser.error("--max-files-per-trigger must be positive")
    return args


def quoted_table(catalog: str, schema: str, table: str) -> str:
    return f"`{catalog}`.`{schema}`.`{table}`"


def create_tables(spark: SparkSession, catalog: str, schema: str) -> dict[str, str]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    tables = {
        "bronze": quoted_table(catalog, schema, "bronze_transactions"),
        "silver": quoted_table(catalog, schema, "silver_transactions"),
        "gold": quoted_table(catalog, schema, "gold_account_risk_windows"),
    }
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {tables['bronze']} (
            transaction_id STRING, account_id STRING, card_id STRING,
            transaction_ts TIMESTAMP, amount DOUBLE, currency STRING,
            merchant_id STRING, merchant_category STRING, channel STRING,
            location_country STRING, event_type STRING, _ingested_at TIMESTAMP
        ) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {tables['silver']} (
            transaction_id STRING, account_id STRING, card_id STRING,
            transaction_ts TIMESTAMP, amount DOUBLE, currency STRING,
            merchant_id STRING, merchant_category STRING, channel STRING,
            location_country STRING, event_type STRING, _ingested_at TIMESTAMP
        ) USING DELTA"""
    )
    spark.sql(
        f"""CREATE TABLE IF NOT EXISTS {tables['gold']} (
            window STRUCT<start: TIMESTAMP, end: TIMESTAMP>, account_id STRING,
            transaction_count BIGINT, total_amount DOUBLE, max_amount DOUBLE,
            suspected_fraud BOOLEAN
        ) USING DELTA"""
    )
    return tables


def start_pipeline(spark: SparkSession, args: argparse.Namespace) -> None:
    tables = create_tables(spark, args.catalog, args.schema)
    source = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{args.checkpoint_path}/schema")
        .option("cloudFiles.maxFilesPerTrigger", args.max_files_per_trigger)
        .schema(TRANSACTION_SCHEMA)
        .load(args.source_path)
    )

    bronze = source.withColumn("_ingested_at", current_timestamp())
    bronze_query = (
        bronze.writeStream.queryName("banking_bronze")
        .outputMode("append")
        .option("checkpointLocation", f"{args.checkpoint_path}/bronze")
        .toTable(tables["bronze"])
    )

    silver = (
        spark.readStream.table(tables["bronze"])
        .withWatermark("transaction_ts", "10 minutes")
        .dropDuplicates(["transaction_id"])
        .where(
            col("transaction_id").isNotNull()
            & col("account_id").isNotNull()
            & col("transaction_ts").isNotNull()
            & (col("amount") >= 0)
        )
        .withColumn("currency", upper(col("currency")))
        .withColumn("channel", upper(col("channel")))
    )
    silver_query = (
        silver.writeStream.queryName("banking_silver")
        .outputMode("append")
        .option("checkpointLocation", f"{args.checkpoint_path}/silver")
        .toTable(tables["silver"])
    )

    risk_windows = (
        spark.readStream.table(tables["silver"])
        .withWatermark("transaction_ts", "10 minutes")
        .groupBy(window("transaction_ts", "5 minutes", "1 minute"), "account_id")
        .agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_max("amount").alias("max_amount"),
        )
        .withColumn(
            "suspected_fraud",
            (col("transaction_count") >= args.velocity_threshold)
            | (col("total_amount") >= args.amount_threshold),
        )
    )
    gold_query = (
        risk_windows.writeStream.queryName("banking_gold_risk_windows")
        .outputMode("append")
        .option("checkpointLocation", f"{args.checkpoint_path}/gold")
        .toTable(tables["gold"])
    )

    print(
        "Started transaction analytics streams: "
        f"{bronze_query.name}, {silver_query.name}, {gold_query.name}"
    )
    spark.streams.awaitAnyTermination()


def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("banking-transaction-analytics").getOrCreate()
    start_pipeline(spark, args)


if __name__ == "__main__":
    main()