# Banking Transaction Analytics

Databricks Asset Bundle for ingesting landed banking transaction events with Auto Loader and maintaining Bronze, Silver, and Gold Delta tables for fraud analysis.

## Data Contract

The job expects newline-delimited JSON files in the configured source directory. Core banking and ATM producers should land events using this common contract:

| Field | Type | Purpose |
| --- | --- | --- |
| `transaction_id` | string | Stable event identifier used for deduplication |
| `account_id` | string | Account used for risk aggregation |
| `card_id` | string | Card identifier |
| `transaction_ts` | timestamp | Event time in ISO-8601 format |
| `amount` | number | Transaction amount, non-negative |
| `currency` | string | ISO currency code |
| `merchant_id` | string | Merchant identifier |
| `merchant_category` | string | Merchant category |
| `channel` | string | Transaction channel, such as ATM or card |
| `location_country` | string | ISO country code |
| `event_type` | string | Source event type |

The Silver table filters incomplete or invalid events, deduplicates by transaction ID with a 10-minute event-time watermark, and normalizes currency and channel to uppercase. The Gold table emits one-minute sliding windows over five-minute account activity; it flags windows meeting either the transaction-count or amount threshold. These thresholds are baseline signals and should be calibrated against labeled fraud outcomes before operational use.

## Tables

- `<catalog>.<schema>.bronze_transactions`: Auto Loader input plus ingestion timestamp.
- `<catalog>.<schema>.silver_transactions`: validated, deduplicated transactions.
- `<catalog>.<schema>.gold_account_risk_windows`: account activity windows and baseline risk flag.

## Deploy

Prerequisites: Databricks CLI configured for the target workspace, serverless jobs enabled, and a Unity Catalog catalog/schema where the job identity can create tables. The source and checkpoint paths must be accessible to that identity; use a Unity Catalog Volume or cloud object storage with the required storage credentials. Keep checkpoint storage durable and do not share it with another pipeline.

Set the bundle variables for your environment, then validate and deploy:

```sh
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run transaction_analytics -t dev
```

Override defaults when deploying, for example:

```sh
databricks bundle deploy -t prod \
	--var="catalog=production, schema_name=banking_analytics, source_path=/Volumes/production/banking_analytics/landing/transactions, checkpoint_path=/Volumes/production/banking_analytics/checkpoints/transactions"
```

The job is configured as a continuously running serverless stream. Stop or pause it from the Databricks Workflows UI when needed. To tune ingestion, override `--max-files-per-trigger`; to tune the baseline fraud signals, pass `--velocity-threshold` and `--amount-threshold` as task parameters.
