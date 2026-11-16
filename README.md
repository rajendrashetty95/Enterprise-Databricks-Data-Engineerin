# Enterprise Data Lakehouse on Azure Databricks

A portfolio/interview-ready reference implementation for a fictional enterprise platform receiving CSV and JSON files from STEP and ORBIT source teams.

> **Important:** This is a runnable learning scaffold, not a deployed production system. It uses synthetic sample data and placeholder ADLS paths. Configure your own Azure storage, credentials, catalog, schemas, and Databricks compute before running cloud ingestion.

## Architecture

STEP / ORBIT (CSV + JSON) -> ADLS Gen2 landing -> Auto Loader -> Bronze Delta -> Silver (standardization, validation, deduplication) -> Gold (business aggregates) -> BI / analytics.

- Metadata-driven source configuration (`config/source_metadata.json`)
- Auto Loader with `cloudFiles`, schema tracking and checkpoint locations
- Bronze retains raw payload and ingestion metadata
- Silver applies type casting, required-field checks and deterministic deduplication
- Invalid rows are written to a quarantine Delta table
- Gold produces daily sales and source-level quality metrics
- Delta `MERGE` example supports idempotent upserts
- SQL DDL, job task outline and unit tests included

## Repository layout

```
config/       Source metadata and environment settings
 data/sample/ Synthetic CSV and JSON input files
notebooks/    Databricks notebook source files (import as notebooks)
src/          Reusable PySpark transformations
sql/          Unity Catalog setup and table DDL
 tests/       Local unit tests for transformation logic
```

## Prerequisites

- Azure Databricks workspace with a supported Databricks Runtime
- ADLS Gen2 account and a Unity Catalog storage credential/external location (recommended)
- Permissions to read landing files and create/write target tables
- Python dependencies: PySpark is provided by Databricks; `pytest` is needed only for local tests

## Configure

1. Upload the repository to Databricks Repos or import the notebook `.py` files.
2. Create a Unity Catalog catalog and schema using `sql/00_setup.sql`.
3. Configure an ADLS Gen2 external location and grant required permissions. Do not hard-code storage keys in notebooks; use Unity Catalog credentials or a secret scope/service principal.
4. Update `config/environment.example.json` and the `base_path` widget in the notebooks to your own ADLS container path.
5. Upload sample files to the matching landing folders, or use your real STEP/ORBIT extracts.
6. Run notebooks in order: `01_bronze_autoloader`, `02_silver_transform`, `03_gold_aggregate`.

## Data contract

Expected logical fields:

- `orders`: `order_id`, `customer_id`, `order_ts`, `product_id`, `quantity`, `unit_price`, `source_system`
- `customers`: `customer_id`, `customer_name`, `email`, `updated_at`, `source_system`

The example input files include only order data to keep the walkthrough small. Extend the metadata and transformations for additional entities.

## Run locally (transformation tests)

The tests are intended for a PySpark-enabled environment. Install pytest and run:

```bash
pytest -q
```

## Production hardening checklist

- Use service principals/managed identity and Unity Catalog external locations; no secrets in source control.
- Separate dev/test/prod catalogs and storage paths.
- Configure job retries, notifications, concurrency limits and task dependencies.
- Add schema contracts, late-arriving data policy, CDC semantics and data retention rules.
- Monitor input file counts, records read/written, quarantine rate, freshness, duration and cost.
- Tune file sizes and table layout based on query patterns; avoid arbitrary partitioning.
- Add CI/CD, code review, data access policies, lineage and recovery procedures.

## Official references

- Auto Loader: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader
- Auto Loader with Unity Catalog: https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/unity-catalog
- Databricks reference architectures: https://docs.databricks.com/aws/en/lakehouse-architecture/reference

The code uses the `cloudFiles` Structured Streaming source and separate schema/checkpoint locations. Check the documentation for runtime- and cloud-specific details before deploying.
