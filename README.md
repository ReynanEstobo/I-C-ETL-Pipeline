# I&C Laundry Management System

I&C Laundry is a centralized management system for the Main, Calzada, and Nasugbu laundry branches. It manages customer records, laundry orders, payments, service statuses, and inventory, with analytics and AI-assisted decision support for branch operations.

## Documentation

- [ETL Stage 1: Data Extraction](docs/extraction/extraction.md)
- [ETL Stage 2: Data Transformation](docs/Transform/transformation.md)
- [ETL Stage 3: Data Load](docs/Load/Load.md)

## Architecture

The Daily Branch Reporting ETL follows the proposed architecture: all processing and storage stay in the one shared Supabase PostgreSQL database, the work is done in SQL, Supabase Cron / `pg_cron` starts it daily at 01:00 Asia/Manila (`0 17 * * *` UTC), and the result is one reporting summary table, `analytics.daily_branch_summary`. Python (`psycopg`) only wraps the same SQL for manual runs, tests, and fixing quarantined records.

| Proposed stage | How it runs | Documented in |
| --- | --- | --- |
| 1. Source | The application's `public.branches`, `customers`, `orders`, `payments`, and `expenses` tables | [Stage 1, Section 2](docs/extraction/extraction.md#2-source-tables-and-column-specification) |
| 2. Ingestion | One read-only snapshot copies the five tables into `pg_temp.stage_*`, under an advisory lock | [Stage 1, Section 1](docs/extraction/extraction.md#1-data-source-and-extraction-specification) |
| 3. Validation | Extraction checks IDs, references, dates, and counts; Stage 2 checks each fact row against the cleaning rules | [Stage 1, Section 3](docs/extraction/extraction.md#3-extraction-validation-and-data-quality-checks), [Stage 2, Section 3](docs/Transform/transformation.md#3-data-cleaning-rules) |
| 4. Cleaning | Stage 2 derives Asia/Manila dates and standardizes statuses on the staged copies; source records are not changed | [Stage 2, Section 4](docs/Transform/transformation.md#4-standardization-and-data-type-rules) |
| 5. Transformation | Stage 2 aggregates orders, payments, and expenses independently by date and branch | [Stage 2, Section 5](docs/Transform/transformation.md#5-business-transformation-rules) |
| 6. Loading and Storage | Load reconciles the batch and publishes it to `analytics.daily_branch_summary` in one transaction | [Stage 3](docs/Load/Load.md) |

The implementation refines the proposal in these ways:

- **Two transactions in one session:** stages 2 to 5 run in one read-only snapshot transaction, and Load publishes in a second, read-write transaction in the same session. Failure logs therefore survive a rollback, and the snapshot cannot write source tables ([Stage 3, Section 1](docs/Load/Load.md#transaction-design)).
- **Quarantine tolerance:** an invalid order, payment, or expense row is set aside instead of stopping the refresh. The batch still publishes when 5% or less of each stream is quarantined; it is held above 5% and rejected above 10% ([Stage 2, Section 10](docs/Transform/transformation.md#10-transformation-acceptance-and-handover-rules)). Schema, reference, and reconciliation failures still stop the refresh.
- **Synchronized publication:** after each publication, the target holds exactly the batch, written with an upsert instead of a truncate and reload. A previously published date and branch is removed only after approval ([Stage 3, Section 3](docs/Load/Load.md#3-load-strategy-and-rules)).
- **Names:** the Cron job is `ic_laundry_daily_extraction`, and it runs `CALL etl_ops.run_scheduled_pipeline();`. These take the place of the proposed `ic_laundry_daily_reports` job and `analytics.refresh_daily_reports()` function.
- **Operations schema:** run logs, publication records, quarantine evidence, and the quarantine fix log are kept in the restricted `etl_ops` schema.
- **Target columns:** besides the proposed columns, each row carries `reporting_cutoff` and `load_run_id` for the completed-day label and lineage.
- **Database account:** runs use the project's `postgres` account; a least-privilege `ic_etl_runtime` role is planned ([Stage 3, Section 1](docs/Load/Load.md#limitations-and-assumptions)).

## Running the pipeline

Copy `.env.example` to `.env` and set the Supabase password before running.

| Command | What it does |
| --- | --- |
| `python -m pipeline.load` | Runs extraction, transformation, and load, then shows a short summary. The full output is saved in `logs/`. |
| `python -m pipeline.load --verbose` | The same run, printing every detail as it happens. |
| `python -m pipeline.quarantine` | Lists the records quarantined by the latest run and how to fix them. `--export` also saves the list as CSV in `quarantine/`. |
| `python -m pipeline.quarantine --fix` | Fixes the quarantined records from the terminal, one by one. Each new value is checked and confirmed before it is saved, and every change is logged. |
| `python -m pipeline.scheduling run` | Runs the scheduled (Cron) pipeline once and shows the same summary. |
| `python -m pipeline.load test` | Tests the load step inside a transaction that is rolled back, so nothing is saved. |

Stage 2 publishes automatically when 5% or less of any transaction stream is quarantined, holds the batch for review above 5%, and rejects it above 10% ([transformation rules](docs/Transform/transformation.md#10-transformation-acceptance-and-handover-rules)).
