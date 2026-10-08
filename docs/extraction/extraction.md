# I&C Laundry Daily Branch Reporting ETL

## Stage 1: Data Extraction

### Objective

The goal of this stage is to extract data from an identified source system and prepare a documented, traceable, and validated dataset for the next stage of the data pipeline.

## 1. Data Source and Extraction Specification

### Source system

I&C Laundry's shared operational system records orders, payments, expenses, customers, and branch information for its Main, Calzada, and Nasugbu branches. These records are the source for daily branch reporting.

### Source database

- **Database:** I&C Laundry operational database
- **DBMS:** PostgreSQL
- **Platform:** Supabase
- **Schema:** `public`

### Extraction method

- **Mode:** Full-history extraction of the selected fields on each run; no incremental cursor is used.
- **Consistency:** Read the source tables in one read-only `REPEATABLE READ` transaction.
- **Output:** Pass the selected source fields to the following pipeline stage without changing their values or row structure.

### Extraction technology

- PostgreSQL / Supabase
- SQL `SELECT` queries through a read-only PostgreSQL connection
- Supabase Cron / `pg_cron` for scheduled orchestration

### Extraction schedule

Run daily at **01:00 Asia/Manila**. If the scheduler uses UTC, use `0 17 * * *`.

### Extraction scope

Extract selected columns from `public.branches`, `public.customers`, `public.orders`, `public.payments`, and `public.expenses`. Include all available history. Retain cancelled orders, soft-deleted expenses, and NULL values for validation and later-stage handling.

### Source limitations and assumptions

- The extraction account must have read access to all five source tables.
- `orders.created_at` may be NULL. Preserve the row and report the missing date.
- Branch references may be NULL. Preserve the row and report the missing reference.
- Payments may be entered after an order is created, and historical records may be corrected. Full-history extraction includes those changes in a later run.
- Extraction volume grows as source history accumulates.

## 2. Source Tables and Column Specification

| Table              | Purpose            | Selected columns and types                                                                                                                                               | Primary key and relationships                                                                         | Reason for inclusion                                                                  |
| ------------------ | ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| `public.branches`  | Branch reference   | `id UUID`; `branch_name TEXT`; `name TEXT NULL`; `code TEXT NULL`                                                                                                        | PK: `id`; referenced by order, payment, and expense branch IDs.                                       | Identifies branches for branch-level reporting.                                       |
| `public.customers` | Customer reference | `id UUID`                                                                                                                                                                | PK: `id`; referenced by `orders.customer_id`.                                                         | Supports customer activity counts without extracting contact details.                 |
| `public.orders`    | Order transactions | `id UUID`; `order_number TEXT`; `branch_id UUID NULL`; `customer_id UUID NULL`; `created_at TIMESTAMPTZ NULL`; `status TEXT`; `weight_kg NUMERIC`; `total_price NUMERIC` | PK: `id`; unique: `order_number`; branch and customer IDs reference `branches.id` and `customers.id`. | Supports order counts, statuses, weight, value, and customer activity.                |
| `public.payments`  | Payment ledger     | `id UUID`; `order_id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `paid_at TIMESTAMPTZ`; `source TEXT`                                                                | PK: `id`; order and branch IDs reference `orders.id` and `branches.id`.                               | Supports payment collection reporting by date and branch.                             |
| `public.expenses`  | Expense records    | `id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `expense_date DATE`; `deleted_at TIMESTAMPTZ NULL`                                                                   | PK: `id`; branch ID references `branches.id`.                                                         | Supports branch expense reporting; soft-deleted rows are retained for later handling. |

Each extracted row retains the grain of its source table. Do not join orders, payments, and expenses during extraction.

## 3. Extraction Validation and Data Quality Checks

| Check name                  | Target                                                           | Purpose                                    | Validation criteria                                                                                                                                          |
| --------------------------- | ---------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Source connection           | PostgreSQL connection                                            | Confirm source accessibility.              | **PASS:** read-only connection succeeds. **FAIL:** connection or authentication fails.                                                                       |
| Required tables and columns | Five source tables and selected columns                          | Detect missing or changed schema.          | **PASS:** all required tables and columns exist with compatible types. **FAIL:** a required table or column is missing or incompatible.                      |
| Required identifiers        | `id` in each table and `orders.order_number`                     | Ensure records can be identified.          | **PASS:** identifiers are present. **FAIL:** a required identifier is NULL.                                                                                  |
| Key uniqueness              | Table `id` fields and `orders.order_number`                      | Detect duplicate records or order numbers. | **PASS:** all values are unique. **FAIL:** duplicate values are found.                                                                                       |
| Relationship check          | Branch, customer, and order references                           | Identify unresolved relationships.         | **PASS:** each non-NULL reference matches a key in its source table. **WARNING:** a nullable reference is NULL. **FAIL:** a non-NULL reference has no match. |
| Record-count completeness   | Source and extracted dataset per table                           | Confirm all snapshot rows were extracted.  | **PASS:** source and output counts match. **FAIL:** counts differ or an output is incomplete.                                                                |
| Date availability           | `orders.created_at`, `payments.paid_at`, `expenses.expense_date` | Identify dates needed by later reporting.  | **PASS:** required dates are present. **WARNING:** nullable `orders.created_at` is NULL. **FAIL:** a required date is missing or invalid.                    |
| Extraction interruption     | Run and output datasets                                          | Detect failed or partial runs.             | **PASS:** all reads and writes complete. **FAIL:** a query or output operation fails; do not hand over partial data.                                         |
| Snapshot consistency        | All five table reads                                             | Ensure reads use one source snapshot.      | **PASS:** all reads use the same `REPEATABLE READ` transaction. **FAIL:** reads use inconsistent snapshots.                                                  |

Record validation results for each table and for the overall run. These checks do not clean or transform data.

## 4. Extraction Metadata and Log Specification

| Field                   | Purpose                                             | Example value or format              |
| ----------------------- | --------------------------------------------------- | ------------------------------------ |
| `pipeline_run_id`       | Identifies and links one run.                       | UUID, e.g. `7d1e...`                 |
| `table_name`            | Identifies the extracted table.                     | `public.orders`                      |
| `extraction_started_at` | Records extraction start time.                      | ISO 8601 timestamp with timezone     |
| `extraction_ended_at`   | Records extraction end time.                        | ISO 8601 timestamp with timezone     |
| `extraction_status`     | Records the run result.                             | `PASS`, `WARNING`, or `FAIL`         |
| `records_read`          | Number of source rows read.                         | Non-negative integer                 |
| `records_extracted`     | Number of rows written to staging.                  | Non-negative integer                 |
| `records_rejected`      | Number not passed to the next stage, if applicable. | Non-negative integer; normally `0`   |
| `extraction_window`     | Identifies the source history included.             | `All available history`              |
| `validation_results`    | Records validation outcomes.                        | JSON, e.g. `{"orders_count":"PASS"}` |
| `error_code`            | Categorizes an extraction error.                    | `SOURCE_TIMEOUT` or `NULL`           |
| `error_message`         | Describes an error for operators.                   | Short diagnostic or `NULL`           |
| `duration_ms`           | Records run duration.                               | Non-negative integer                 |

Use the metadata to monitor runs, troubleshoot failures, audit extraction activity, and support recovery. A retry receives a new `pipeline_run_id`.

## 5. Log Retention and Access

- **Retention:** Keep operational extraction logs for at least one year to support monitoring and recovery.
- **Storage:** Store logs in a restricted ETL operations log store, separate from source records.
- **Access:** Limit log access to authorized ETL operators and system administrators. Restrict source credentials to the extraction runtime and authorized database administrators.
- **Archiving:** Archive logs when needed for audit or recovery, preserving run IDs, timestamps, statuses, and validation results.
- **Deletion:** Delete logs only after the retention period and only when they are no longer needed for troubleshooting, auditing, or recovery. An authorized ETL or database administrator performs and records deletion.
- **Audit records:** Apply any longer organizational retention requirement to audit records separately from operational logs.

## 6. Extraction Data Contract and Naming Convention

### Required schema

The output contains the selected columns listed in Section 2. Preserve source identifiers, relationships, data types, values, and NULLs. Each table remains a separate dataset in its original row grain.

### Naming convention

- Preserve lowercase `snake_case` source table and column names.
- Keep primary keys named `id` and preserve foreign-key names such as `branch_id`, `customer_id`, and `order_id`.
- Preserve UUID and exact `NUMERIC` values, dates, timestamps, text, and NULLs.

### Source-to-extract mapping

Stage 1 does not standardize field names. Each selected column maps to an identically named output column with its source type and value.

| Source column           | Output column  | Source type        | Target type        | Mapping type |
| ----------------------- | -------------- | ------------------ | ------------------ | ------------ |
| `orders.created_at`     | `created_at`   | `TIMESTAMPTZ NULL` | `TIMESTAMPTZ NULL` | Preserve     |
| `payments.amount`       | `amount`       | `NUMERIC`          | `NUMERIC`          | Preserve     |
| `expenses.expense_date` | `expense_date` | `DATE`             | `DATE`             | Preserve     |

### Schema consistency

Check required tables, columns, data types, and identifiers before extraction. Stop the run if a required field is missing, renamed, or incompatible. Review schema changes before updating this contract.

## 7. Extraction Acceptance and Handover Rules

| Validation condition         | Acceptance criteria                                                                                         | Pipeline action                                                                                           | Decision / responsible role                        |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| All critical checks pass     | Required tables and columns exist; identifiers, uniqueness, row counts, snapshot, and output checks pass.   | Mark `APPROVED` and continue to Transformation.                                                           | Automatic.                                         |
| Non-critical warnings remain | Nullable dates or references are reported, but extraction is complete.                                      | Hold for review; preserve the data unchanged.                                                             | `MANUAL REVIEW` by an ETL operator or data owner.  |
| A critical check fails       | Required source data is unavailable, identifiers are invalid, counts mismatch, or extraction is incomplete. | Mark `REJECTED`; stop handover and quarantine partial output. Retry transient failures with a new run ID. | Automatic stop; ETL operator resolves the failure. |

### Handover

Pass the five staged datasets, run metadata, validation results, errors, and acceptance status to Transformation. The next stage acknowledges the run ID and status. Cleaning, standardization, filtering, and business calculations are not part of this extraction stage.

## 8. Common Extraction Problems and Mitigation

| Problem                               | Possible cause                                           | Impact                                                | Proposed mitigation                                                          |
| ------------------------------------- | -------------------------------------------------------- | ----------------------------------------------------- | ---------------------------------------------------------------------------- |
| Database connection failure           | Network or database outage                               | Extraction cannot complete.                           | Retry transient failures; stop and report persistent failures.               |
| Read permission failure               | Invalid credentials or changed grants                    | One or more tables cannot be extracted.               | Verify read access for each required table.                                  |
| Schema change                         | Required table or column is renamed, removed, or changed | Query fails or output no longer matches the contract. | Stop extraction, review the change, and update the contract before retrying. |
| Interrupted extraction                | Worker, connection, or output failure                    | Partial datasets may be handed over accidentally.     | Mark the run failed, quarantine partial output, and retry with a new run ID. |
| Source/output count mismatch          | Truncated read or write                                  | Records may be missing or duplicated.                 | Compare counts per table; reject mismatched output.                          |
| Missing or duplicate identifiers      | Source integrity or extraction error                     | Records cannot be reliably identified or related.     | Record the affected table and stop acceptance until resolved.                |
| Late payment or historical correction | A source record changes after an earlier run             | Previous reporting periods may change.                | Re-extract full history on the next run.                                     |
| Incorrect schedule timezone           | Scheduler is not set to Asia/Manila or UTC equivalent    | Extraction runs at the wrong time.                    | Verify the scheduler timezone and cron setting.                              |

## Expected Output

Five extracted datasets in transaction-local staging, together with the run metadata, validation results, and acceptance status required for handover to Transformation.
