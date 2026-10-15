# I&C Laundry Daily Branch Reporting ETL

## Stage 1: Data Extraction

### Objective

Extract the source records needed for daily branch reporting and prepare a traceable, validated dataset for the next pipeline stage. Extraction preserves the original records. Cleaning, standardization, calculations, and loading reporting summaries are outside this stage.

## 1. Data Source and Extraction Specification

### Source system and database

I&C Laundry uses a shared operational system for its Main, Calzada, and Nasugbu branches. The source is the existing Supabase PostgreSQL database, with operational and reference tables in the `public` schema.

### Extraction method and tools

Extraction must read full history on each run, with no incremental cursor. SQL must copy the selected source columns into separate temporary staging tables inside the same PostgreSQL database.

Python will wrap the SQL extractor using `psycopg`. The wrapper will manage the connection and transaction, execute SQL, and display counts and validation results. Extracted records must remain in PostgreSQL rather than being downloaded into Python memory.

Empty temporary tables are created in a setup transaction. Source reads, staging inserts, and validation then run in one read-only `REPEATABLE READ` transaction. Only temporary staging tables receive extracted records; source tables remain unchanged.

### Schedule

Extraction must run daily at **01:00 Asia/Manila**, using `0 17 * * *` when the scheduler uses UTC. Supabase Cron / `pg_cron` will schedule the extraction SQL inside PostgreSQL. Python will wrap the SQL for manual execution and testing.

Manual and scheduled runs must follow the same extraction scope, snapshot requirements, and validation rules.

### Extraction scope

Extract the columns in Section 2 from `public.branches`, `public.customers`, `public.orders`, `public.payments`, and `public.expenses`. Include all available history, cancelled orders, soft-deleted expenses, and NULL values. Keep each table at its original row grain.

Do not filter, clean, standardize, or calculate business values during extraction.

### Limitations and assumptions

- The account needs SELECT access to the required source columns and permission to create temporary tables.
- Required source columns and types must match Section 2. Unexpected changes stop extraction.
- Nullable dates and references are retained and reported. Warnings require review before handover.
- Full-history extraction includes late payments and historical corrections on subsequent runs, but its volume grows over time.
- Temporary stages are available only in the current transaction and connection. They cannot be retained for review after the session closes.

## 2. Source Tables and Column Specification

Columns marked NULL are nullable. Other listed columns are required by the source contract. Source NUMERIC values have no declared precision or scale.

| Source table | Purpose and reason for inclusion | Selected columns and types | Keys and relationships |
| --- | --- | --- | --- |
| `public.branches` | Identifies branches; `branch_name` supplies reporting labels. | `id UUID`; `branch_name TEXT`; `name TEXT NULL`; `code TEXT NULL` | PK: `id`; referenced by order, payment, and expense branch IDs. |
| `public.customers` | Supports customer activity counts without extracting contact details. | `id UUID` | PK: `id`; referenced by `orders.customer_id`. |
| `public.orders` | Supplies order activity, statuses, weight, and order value. | `id UUID`; `order_number TEXT`; `branch_id UUID NULL`; `customer_id UUID NULL`; `created_at TIMESTAMPTZ NULL`; `status TEXT`; `weight_kg NUMERIC`; `total_price NUMERIC` | PK: `id`; unique identifier: `order_number`; references `branches.id` and `customers.id`. |
| `public.payments` | Supplies individual payment collections by date and branch. | `id UUID`; `order_id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `paid_at TIMESTAMPTZ`; `source TEXT` | PK: `id`; references `orders.id` and `branches.id`. One order can have several payments. |
| `public.expenses` | Supplies branch expenses and deletion status for later filtering. | `id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `expense_date DATE`; `deleted_at TIMESTAMPTZ NULL` | PK: `id`; references `branches.id`. |

`orders.order_number` is included as an extraction identifier for completeness and uniqueness checks. Orders, payments, and expenses are not joined during extraction.

## 3. Extraction Validation and Data Quality Checks

SQL must check the staged records inside PostgreSQL. During manual runs, Python must display the returned check results. These checks must report problems without repairing, removing, or standardizing records.

| Check | Target | Purpose | Validation criteria |
| --- | --- | --- | --- |
| Source accessibility | Connection and selected source columns | Confirm the source can be read. | PASS: connection and SELECT access succeed. FAIL: connection, authentication, or access fails. |
| Schema compatibility | Five tables and selected columns | Detect missing or changed fields. | PASS: required columns exist with the expected types. FAIL: a field is missing, inaccessible, or incompatible. |
| Required identifiers | All `id` fields and `orders.order_number` | Identify every record. | PASS: no NULL identifiers. FAIL: any required identifier is NULL. |
| Uniqueness | All `id` fields and `orders.order_number` | Detect duplicate identifiers. | PASS: no duplicates. FAIL: any non-NULL identifier is repeated. |
| References | Branch, customer, and order references | Detect unresolved relationships. | PASS: non-NULL references match staged parent IDs. WARNING: a nullable reference is NULL. FAIL: a non-NULL reference has no match, or required `payments.order_id` is NULL. |
| Date availability and validity | `orders.created_at`, `payments.paid_at`, `expenses.expense_date` | Confirm dates are available for the next stage. | PASS: dates are present and finite. WARNING: nullable `orders.created_at` is NULL. FAIL: a required date is NULL or a date is infinite. PostgreSQL column types reject malformed dates. |
| Record-count completeness | Source and staged counts for each table | Detect missing or additional staged rows. | PASS: counts match within the same snapshot. FAIL: counts differ. Empty sources with empty stages pass. |
| Snapshot consistency | All source reads | Prevent mixed source versions. | PASS: reads and checks use one read-only `REPEATABLE READ` transaction. FAIL: required transaction settings are not active. |
| Completion | Entire extraction run | Prevent partial handover. | PASS: all staging and checks finish. FAIL: a query, connection, or staging operation fails. |

Record per-check, per-table, and overall results. A table fails if any of its checks fail, otherwise it receives WARNING if any check warns, or PASS. The overall result follows the same precedence. Checks must not change the extracted data.

## 4. Extraction Metadata and Log Specification

| Field | Purpose | Example or format |
| --- | --- | --- |
| `pipeline_run_id` | Links all records for one run. | UUID |
| `table_name` | Identifies the source table. | `public.orders` |
| `extraction_started_at` | Records extraction start. | TIMESTAMPTZ |
| `extraction_ended_at` | Records extraction completion or failure. | TIMESTAMPTZ |
| `extraction_status` | Records the validation outcome. | `PASS`, `WARNING`, `FAIL` |
| `records_read` | Counts source rows in the snapshot. | Non-negative integer; NULL if unavailable |
| `records_extracted` | Counts rows copied into staging. | Non-negative integer; NULL if unavailable |
| `records_rejected` | Counts individually excluded rows, if applicable. | `0`; records are not filtered during extraction |
| `extraction_window` | Defines the source history read. | `All available history` |
| `validation_results` | Stores check and table outcomes with affected counts. | JSONB |
| `error_code` | Identifies the failure category. | `SOURCE_CONNECTION_FAILED`, `SCHEMA_MISMATCH`, or NULL |
| `error_message` | Describes the failure without credentials. | TEXT or NULL |
| `duration_ms` | Measures extraction runtime. | Non-negative integer |
| `acceptance_status` | Records the handover decision separately from validation. | `APPROVED`, `MANUAL REVIEW`, `REJECTED` |

Capture one run-start timestamp for the source snapshot and link table logs using the run ID. Retries receive new run IDs. Logs support monitoring, troubleshooting, auditing, and recovery; unavailable counts must not be reported as zero.

Store logs in a restricted ETL operations table in the same PostgreSQL database, separate from `public` source records. During manual runs, Python must display staged counts and validation results. Failure logs must survive rollback of the extraction transaction. A separate logging transaction must store only run metadata, validation results, and error details; it must not extract or process source records. If the database is unavailable, record the failure once connectivity is restored, using the original run ID and timestamps.

The Python wrapper must keep undelivered log metadata in a private local `.pending-extraction-logs` directory so it survives restarts. This queue must contain no credentials or extracted source records and must be excluded from Git. Replay pending entries before the next extraction, or with `python -m pipeline.extraction --flush-logs`. Keep original run IDs and timestamps, prevent duplicate database entries, and remove a queued entry only after its database transaction commits. The queue is temporary metadata storage, not an extraction staging store.

Scheduled runs must commit metadata to the restricted `etl_ops.pending_extraction_logs` table before delivering it to the final log table. Replay must preserve run IDs and timestamps, avoid duplicate entries, and remove pending rows only in the transaction that confirms final storage. Replay occurs before the next scheduled extraction or through `python -m pipeline.scheduling recover`. This queue contains metadata only. If PostgreSQL is entirely unavailable or the connection ends before pending metadata commits, in-database recovery cannot guarantee a complete extraction log; the operator must inspect Cron history and database service logs, record the available failure evidence after recovery, and rerun extraction with a new run ID.

## 5. Log Retention and Access

- **Retention:** Keep extraction logs for at least one year to investigate failed runs, historical corrections, and reporting discrepancies.
- **Storage:** Use the restricted ETL operations log table in the existing PostgreSQL database. Temporary staging is not a log store.
- **Access:** Allow authorized ETL operators and database administrators to read logs. Restrict log writes and source credentials to authorized runtimes and administrators.
- **Pending logs:** Restrict the local queue to the operating-system account running the wrapper. Retain undelivered entries until database storage is confirmed; the one-year retention period applies to the saved database logs.
- **Scheduled pending logs:** Restrict the database metadata queue to approved logging roles. Retain pending rows until final-log delivery is confirmed; source records must never be stored there.
- **Archiving:** Archive older logs within restricted PostgreSQL storage when needed, retaining run IDs, timestamps, results, and errors.
- **Deletion:** An authorized database administrator checks retention and open investigations, then records any deletion. Never delete logs before one year or while needed for troubleshooting, auditing, or recovery.
- **Audit records:** Follow any longer organizational retention requirement separately from operational extraction logs.

### Access procedure

The database administrator must approve access before granting it. ETL operators need read access for monitoring; the extraction runtime needs permission to insert logs. Other application users must not receive access. Log setup restricts access to the table owner by default. Additional grants and any required row-level security policies must be limited to approved database roles and verified using those roles. Review access when a team member changes responsibilities or leaves the project.

### Archive procedure

Archiving is performed only when needed; keeping logs in the original table is acceptable. Before archiving, the database administrator must identify the affected run IDs and check for open troubleshooting, audit, or recovery work with the ETL operator or data owner. Copy the complete log records into restricted archive storage within PostgreSQL, then verify row counts, run IDs, timestamps, validation results, and errors. Record the archive location, date, responsible administrator, and verification result in the approved maintenance record. An archive copy must follow the same access and retention rules. Archiving does not authorize deletion or shorten retention.

### Review and deletion procedure

1. The database administrator identifies logs that have completed the one-year retention period. Pending local logs are not eligible until their database delivery is confirmed.
2. The ETL operator or data owner confirms that the selected run IDs are no longer needed for troubleshooting, auditing, or recovery. If that confirmation is unavailable, retain the logs and review them later.
3. Record the run IDs, row count, retention dates, reason, approval, and responsible administrator in the approved maintenance record before deletion. Include any archive location and verification result.
4. The database administrator deletes only the approved records in a transaction, checks the affected count, and rolls back if it differs from the approved count. Retention protection must not be disabled to bypass the minimum period.
5. Record the completion time and actual deleted count, or the rollback outcome. Keep the maintenance record under the applicable audit retention policy, separately from the deleted operational logs.

The retention trigger enforces the minimum age; it does not determine whether a run is still under investigation. The review and approval steps remain mandatory even when the trigger allows deletion.

## 6. Extraction Data Contract and Naming Convention

### Required schema and naming

| Source | Temporary output |
| --- | --- |
| `public.branches` | `pg_temp.stage_branches` |
| `public.customers` | `pg_temp.stage_customers` |
| `public.orders` | `pg_temp.stage_orders` |
| `public.payments` | `pg_temp.stage_payments` |
| `public.expenses` | `pg_temp.stage_expenses` |

Each output contains exactly the selected columns in Section 2 with the same names, types, values, and NULLs. Preserve lowercase `snake_case`, `id` primary-key names, and foreign-key names such as `branch_id`, `customer_id`, and `order_id`. The `stage_` prefix identifies temporary extraction tables; no new column abbreviations are introduced.

UUIDs, exact NUMERIC values, text, dates, and timestamps remain in PostgreSQL. Staging tables do not enforce source key or NOT NULL constraints, so invalid values can be copied and reported without silent removal. The validation checks enforce the acceptance requirements.

### Source-to-output mapping

All selected columns map directly to identically named staging columns. No renaming or type conversion is performed.

| Source column | Staged column | Source type | Output type | Mapping |
| --- | --- | --- | --- | --- |
| `orders.created_at` | `stage_orders.created_at` | TIMESTAMPTZ NULL | TIMESTAMPTZ NULL | Preserve |
| `payments.amount` | `stage_payments.amount` | NUMERIC | NUMERIC | Preserve |
| `expenses.expense_date` | `stage_expenses.expense_date` | DATE | DATE | Preserve |

### Schema consistency

Check required columns and types before copying records. Stop if a required field is missing, renamed, or incompatible. Review the source change before updating queries and the contract. Added source columns outside the extraction scope do not change the output.

## 7. Extraction Acceptance and Handover Rules

| Condition | Acceptance criteria | Pipeline action | Decision and responsible role |
| --- | --- | --- | --- |
| All checks pass | All five datasets are complete, with no failed checks or warnings. | Mark `APPROVED`; allow the next stage to use staging in the same transaction. | Automatic. |
| Non-critical warnings | Extraction is complete, but nullable dates or references need review. | Mark `MANUAL REVIEW`; stop handover, retain diagnostic logs, and discard temporary staging when the run ends. Rerun after review. | ETL operator or data owner reviews the source issue. |
| Critical failure | Required source access, schema, identifiers, references, dates, counts, or completion checks fail. | Mark `REJECTED`; stop handover and roll back staging. Retain failure logs. Retry temporary failures with a new run ID. | Automatic stop; ETL operator or database administrator resolves the failure. |

Manual review must resolve the warning or record an approved explanation before a new extraction is assessed for handover. Critical failures must be resolved before approval.

### Handover

Handover must provide the five temporary stages, run ID, run timestamp, metadata, validation results, errors, and acceptance status. The next stage must acknowledge the run ID and decision within the same database transaction before consuming approved staged records. Held or rejected runs must retain diagnostic logs, discard staging, and require a new extraction after the issue is resolved. No file export or external staging store is used.

A standalone extraction check through Python may display counts and validation results, then close the connection and remove temporary tables. This check is not a handover. Handover must occur while the temporary stages are still available. Cleaning, standardization, and business transformations are outside this stage.

Scheduled handover uses an optional receiver configured by the database administrator in `etl_ops.extraction_handover`. The receiver provides two SQL functions accepting and returning JSONB: acknowledgment of the run ID and acceptance status, followed by consumption of the full metadata and staging references. Both functions must use invoker permissions and execute within the extraction transaction. Invalid acknowledgment blocks consumption and rejects the run. With no receiver configured, the scheduled run is an extraction check only; its logs must identify handover as `NOT_CONFIGURED`, rather than claim that data was handed over. A real next stage must be configured before end-to-end handover is considered complete.

## 8. Common Extraction Problems and Mitigation

| Problem | Possible cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Connection failure | Unreachable Supabase endpoint, invalid credentials, or database outage. | No complete snapshot can be extracted. | Check the connection configuration; use the session pooler where direct connectivity is unavailable. Retry temporary failures with a new run ID. |
| Permission failure | Missing SELECT or temporary-table privileges. | Source checks or staging cannot finish. | Stop the run and have a database administrator restore the required access. |
| Source schema change | Required fields are removed, renamed, or change type. | Queries or output no longer match the contract. | Reject the run; review and update the approved contract before retrying. |
| Interrupted extraction | Connection loss or SQL execution failure. | Staging may be incomplete. | Roll back staging, block handover, retain failure logs, and retry temporary failures. |
| Count mismatch | Incomplete or incorrect staging query. | Extracted rows are missing or duplicated. | Reject the run; compare source and staging counts in the same snapshot and fix the extraction query. |
| Invalid identifiers, references, or dates | Missing source values or inconsistent records. | Extracted data does not meet acceptance criteria. | Record the failed or warning checks; hold or reject the run without editing source records. |
| Late payments or historical corrections | Existing history changes after a successful run. | Earlier extracted history becomes outdated. | Extract full history again on the next run. |
| Overlapping scheduled runs | A previous extraction has not finished. | Repeated full-history reads increase database load. | Prevent concurrent scheduled extractions with a transaction-scoped advisory lock. |
| Incorrect schedule | Cron timezone or expression is wrong. | Extraction runs at the wrong business time. | Verify the UTC schedule corresponds to 01:00 Asia/Manila before enabling the job. |

## Expected Output

Five raw, validated datasets in PostgreSQL temporary staging at their original row grain, with run metadata, validation results, and an acceptance decision for handover within the same transaction. Source records must remain unchanged, and persistent logs must remain available after staging is removed. Python will wrap the extraction SQL without moving source records out of PostgreSQL.
