# I&C Laundry ETL Pipeline — Stage 1: Data Extraction

## Objective

The goal of this stage is to extract data from an identified source system and prepare a documented, traceable, and validated dataset for the next stage of the data pipeline.

## 1. Data Source and Extraction Specification

### Source system

The source is the I&C Laundry operations system serving the Main, Calzada, and Nasugbu branches. It records customer IDs, laundry orders and statuses, payment collections, expenses, and branch references in Supabase PostgreSQL.

### Source database

- **DBMS:** PostgreSQL hosted through Supabase.
- **Source schema:** `public`.
- **Source tables:** `branches`, `customers`, `orders`, `payments`, and `expenses`.
- **Access assumption:** An authorized ETL identity can read the required source tables. Use a dedicated, least-privilege read-only identity; do not place a database service-role secret in an end-user client.

### Extraction method

- **Mode:** Full snapshot of the five listed tables on each scheduled run. This is a proposed design choice to keep historical corrections and late payment entries in scope; review it if source volume makes daily full reads impractical.
- **Consistency:** Read all tables from one PostgreSQL transaction snapshot. Preserve source rows and table grain; do not join transaction tables during extraction.
- **Tool/technology:** PostgreSQL SQL queries run by the ETL job through an authorized Supabase/PostgreSQL connection. The specific client library or script is not yet selected. Export only the columns in Section 2, with one raw dataset per table. Record the output format and preserve NULLs, exact numeric values, UUIDs, and timestamp offsets.
- **Filtering:** Keep cancelled orders, soft-deleted expenses, null dates, and null branch IDs in the raw extract. Later stages decide which records to use and how to transform them.
- **Incremental cursor:** None; this design does not use a high-water mark.
- **Schedule:** Daily at **01:00 Asia/Manila (UTC+08:00)**, as proposed in the technical metadata. If the scheduler uses UTC, the corresponding cron time is `17:00 UTC` on the previous UTC date (`0 17 * * *`). Verify the actual scheduler timezone before enabling it.
- **Business-date cutoff:** Extraction does not filter by business date. Preserve source dates and timestamps so the next stage can apply its documented local-date cutoff.

### Extraction scope

Extract all available historical rows and only the columns listed in Section 2. The scope supports branch-level reporting of orders, order value, payment collections, expenses, order weight/status, and distinct customer activity. Customer contact information is not required; only the customer ID is included.

Do not export unrelated tables, application secrets, or customer names, phone numbers, and email addresses.

### Source limitations and assumptions

1. The listed schema comes from project documentation, not a live database inspection. Check tables, columns, types, nullability, keys, relationships, and permissions before implementation.
2. The metadata lists `branches.branch_name`; project migrations also retain `branches.name` and synchronize the two. Confirm the deployed columns and agree on the label field before extraction.
3. Several branch, customer, or event-date fields may be NULL. Preserve those rows and flag them; unresolved branch/date values may need review before transformation.
4. A consistent snapshot ensures the tables are read from one point in time. It does not prove the source records are accurate or complete.
5. Preserve historical values as stored. Do not infer missing dates or rewrite records during extraction.
6. Daily full snapshots grow as history grows. Monitor runtime and database load; document a late-arriving-data policy before changing to incremental extraction.
7. The schedule and access model are proposed. Confirm whether Supabase Cron/pg_cron and the required grants are configured.

## 2. Source Tables and Column Specification

Types and relationships follow the supplied ETL metadata and must be checked against the deployed schema. `NULL` marks a nullable field. Numeric precision and scale were not specified.

| Source table       | Purpose in this extraction          | Selected columns and source types                                                                                                                   | Key and relationships                                                                                            | Reason for inclusion                                                                                                           |
| ------------------ | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| `public.branches`  | Branch reference data               | `id UUID`; `branch_name TEXT`; `name TEXT NULL`; `code TEXT NULL`                                                                                   | `id` primary key. Referenced by branch IDs in operational records.                                               | Provides branch IDs and labels for later grouping. Confirm the deployed label column.                                          |
| `public.customers` | Customer reference data             | `id UUID`                                                                                                                                           | `id` primary key. Referenced by `orders.customer_id`.                                                            | Supports distinct-customer counts without exporting contact details.                                                           |
| `public.orders`    | Laundry orders and service statuses | `id UUID`; `branch_id UUID NULL`; `customer_id UUID NULL`; `created_at TIMESTAMPTZ NULL`; `status TEXT`; `weight_kg NUMERIC`; `total_price NUMERIC` | `id` primary key; `branch_id → branches.id`; `customer_id → customers.id`. One order may have multiple payments. | Supports order counts, released-order counts, customer activity, weight, and order value. Retain every status for later rules. |
| `public.payments`  | Payment collection ledger           | `id UUID`; `order_id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `paid_at TIMESTAMPTZ`; `source TEXT`                                           | `id` primary key; `order_id → orders.id`; `branch_id → branches.id`. One order may have multiple payment rows.   | Supports collection-date revenue and payment-entry counts while preserving ledger row grain.                                   |
| `public.expenses`  | Branch expense records              | `id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `expense_date DATE`; `deleted_at TIMESTAMPTZ NULL`                                              | `id` primary key; `branch_id → branches.id`.                                                                     | Supports expense reporting. Include soft-deleted rows for later-stage filtering.                                               |

### Extraction row-grain rules

- One output row represents exactly one source row from the same-named source table.
- Do not join `orders` directly to `payments` or `expenses` during extraction; one-to-many relationships can duplicate source rows.
- Preserve source identifiers, source column names, values, NULLs, and statuses. Any future alias or type conversion must be added to the data contract and treated as a documented mapping.

## 3. Extraction Validation and Data Quality Checks

These checks confirm the source snapshot was read and packaged correctly. They do not clean data or calculate business metrics.

| Check name                        | Target                                                                         | Purpose                                                                  | Pass/fail criteria                                                                                                                                                                                                              |
| --------------------------------- | ------------------------------------------------------------------------------ | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Source connection and read access | PostgreSQL connection and five source tables                                   | Confirm the job can reach and read the expected source.                  | **PASS:** connection and read-only probes succeed for all tables. **FAIL:** timeout, authentication/permission error, wrong database, or inaccessible table.                                                                    |
| Required schema present           | Tables and columns in Section 2                                                | Detect schema drift before extraction.                                   | **PASS:** required tables/columns, types, and keys match the approved contract. **FAIL:** a required table/column is missing or incompatible. Record branch-label variants and resolve the agreed label before extraction.      |
| Source identifiers present        | `id` on all five tables; `orders.id`, `customers.id`, and `branches.id`        | Ensure rows have stable identifiers for traceability and joins.          | **PASS:** zero NULL primary IDs in extracted rows. **FAIL:** one or more missing source primary IDs.                                                                                                                            |
| Primary-key uniqueness            | Each extracted table's `id`                                                    | Detect duplicate source rows/keys or packaging duplication.              | **PASS:** distinct `id` count equals extracted row count for each table. **FAIL:** mismatch.                                                                                                                                    |
| Extraction completeness           | Each source table and its output dataset                                       | Confirm all rows visible in the run snapshot were emitted.               | **PASS:** source count equals output count per table, and source/output counts are recorded. **FAIL:** mismatch, missing dataset, truncated output, or interrupted serialization.                                               |
| Relationship coverage             | Order, payment, and expense foreign keys against reference extracts            | Find unresolved references without changing or dropping rows.            | **PASS:** every non-NULL reference matches an extracted key. **WARNING:** a nullable reference is NULL; keep the row and flag it. **FAIL:** a non-NULL foreign key has no matching extracted key.                               |
| Reporting date availability       | `orders.created_at`, `payments.paid_at`, `expenses.expense_date`               | Identify records that cannot be assigned to a reporting date downstream. | **PASS:** zero null/invalid required event dates. **WARNING:** any missing/invalid event date; preserve row and require review. Do not impute a date in extraction.                                                             |
| Numeric serialization fidelity    | `orders.weight_kg`, `orders.total_price`, `payments.amount`, `expenses.amount` | Prevent precision loss or malformed values during export.                | **PASS:** exported value parses as a PostgreSQL-compatible numeric and round-trips without rounding. **FAIL:** parse error, overflow, or value changed by serialization. No business range checks are performed here.           |
| Extraction interruption           | Run state and per-table output                                                 | Detect partial runs.                                                     | **PASS:** all table reads, writes, and validation finish and the package is marked complete. **FAIL:** any query/serialization/upload fails or the run ends without finalization; partial package is not eligible for handover. |
| Snapshot identity                 | Run metadata                                                                   | Ensure all tables belong to one documented source snapshot.              | **PASS:** one transaction snapshot/run ID applies to all tables. **FAIL:** tables were read across inconsistent snapshots or snapshot metadata is absent.                                                                       |

Report counts and warnings per table. A warning never authorizes dropping or changing a source row.

## 4. Extraction Metadata and Log Specification

Write one run record and one record per table. Keep logs separate from extracts; never log credentials or customer contact details.

| Field                                               | Purpose                                                                                                                              | Example value or format                                                                                                  |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------ |
| `pipeline_run_id`                                   | Unique trace ID shared by the run, outputs, and downstream handover.                                                                 | UUID, e.g. `7d1e...`                                                                                                     |
| `pipeline_name`                                     | Identifies the job.                                                                                                                  | `ic_laundry_daily_branch_reports`                                                                                        |
| `source_system`                                     | Identifies the source application.                                                                                                   | `I&C Laundry Supabase PostgreSQL`                                                                                        |
| `source_database`                                   | Identifies the expected source DB/project without exposing secrets.                                                                  | Non-secret project/database identifier                                                                                   |
| `table_name`                                        | Identifies a table-level extraction.                                                                                                 | `public.orders`                                                                                                          |
| `snapshot_started_at`                               | Start time of the consistent snapshot.                                                                                               | ISO 8601 timestamp with timezone                                                                                         |
| `extraction_started_at`                             | Start time for the table/run extraction.                                                                                             | ISO 8601 timestamp with timezone                                                                                         |
| `extraction_ended_at`                               | End time for the table/run extraction.                                                                                               | ISO 8601 timestamp with timezone                                                                                         |
| `extraction_status`                                 | Run/table outcome.                                                                                                                   | `RUNNING`, `PASS`, `WARNING`, or `FAIL`                                                                                  |
| `records_read`                                      | Number of rows visible from the source query/snapshot.                                                                               | Non-negative integer                                                                                                     |
| `records_extracted`                                 | Number of rows successfully serialized into the output dataset.                                                                      | Non-negative integer                                                                                                     |
| `records_rejected`                                  | Rows withheld from the handover package, if any. The preferred extraction behavior is to retain rows and flag them, not reject them. | Non-negative integer; normally `0`                                                                                       |
| `extraction_window_start` / `extraction_window_end` | Identifies an incremental range if one is introduced.                                                                                | `NULL` for the current full-extract design; otherwise ISO timestamps/dates with inclusive/exclusive semantics documented |
| `validation_results`                                | Machine-readable per-check outcome and counts.                                                                                       | JSON object, e.g. `{"pk_unique":"PASS","date_availability":"WARNING:2"}`                                                 |
| `output_location`                                   | Resolves the emitted table dataset/manifest.                                                                                         | Non-secret URI/path/object key or controlled run-package ID                                                              |
| `output_format`                                     | Records the physical serialization.                                                                                                  | E.g. `CSV UTF-8`; exact implementation value                                                                             |
| `error_code`                                        | Stable error category for automated monitoring.                                                                                      | E.g. `SOURCE_TIMEOUT`, `SCHEMA_MISMATCH`                                                                                 |
| `error_message`                                     | Safe diagnostic details; redact secrets and personal data.                                                                           | Short sanitized message, or `NULL`                                                                                       |
| `duration_ms`                                       | Supports performance monitoring and alerting.                                                                                        | Non-negative integer                                                                                                     |
| `source_row_count` / `output_checksum`              | Provides source-to-output reconciliation evidence.                                                                                   | Integer count and SHA-256 digest per table/file                                                                          |

Use the logs to monitor runs, reconcile row counts, detect schema/access changes, diagnose interruptions, and decide when to retry. Link each package to its checks with `pipeline_run_id` and snapshot metadata. Every retry gets a new run ID; incomplete packages are never accepted.

## 5. Log Retention and Access

- **Proposed retention:** Keep operational logs for at least **1 year** for routine monitoring and recovery. Extend retention for open incidents, audits, or recovery work.
- **Storage:** Use a restricted ETL operations log store. Select and record the actual storage location before deployment; do not use a public bucket or frontend assets.
- **Access:** The ETL identity writes run metadata and extracts. Designated pipeline operators and administrators read logs and validation results. Restrict source credentials to the job runtime and authorized database administrators.
- **Archiving:** Archive logs only when needed for an approved audit or recovery purpose. Preserve run IDs, timestamps, checks, counts, and checksums, and protect archives with equivalent access controls and encryption.
- **Deletion:** An authorized administrator may delete logs after the retention period only when no troubleshooting, audit, or recovery hold remains. Record the deletion date, scope, and responsible role. If storage is low, alert and expand or rotate it; do not delete logs early.
- **Data minimization:** Logs contain metadata, counts, checks, and sanitized errors—not customer contact details, passwords, or credentials.

If organizational policy requires longer retention for audit records, follow that policy separately; the one-year operational-log period does not override it.

## 6. Extraction Data Contract and Naming Convention

### Required schema and naming

- Keep source table and selected column names unchanged in the raw extracts.
- Preserve the PostgreSQL types listed in Section 2: `UUID`, `TEXT`, `TIMESTAMPTZ`, `DATE`, and `NUMERIC`.
- Serialize `NUMERIC` without floating-point conversion or rounding. Retain timestamp timezone information; do not derive business dates here.
- Keep NULL distinct from an empty string and document the serialization rules in the manifest.
- Retain source primary and foreign keys; do not create replacement IDs.
- Names use lowercase `snake_case`. Do not abbreviate or rename source columns during extraction.
- Use branch UUIDs as identifiers. Use the branch label column confirmed during schema preflight.

### Source-to-extract field mapping

This stage copies source names and types unchanged. The mapping records that one-to-one contract; it does not define business transformations.

| Source column           | Extracted column | Source type        | Extracted type     | Mapping type                                  |
| ----------------------- | ---------------- | ------------------ | ------------------ | --------------------------------------------- |
| `branches.id`           | `id`             | `UUID`             | `UUID`             | None; preserved                               |
| `branches.branch_name`  | `branch_name`    | `TEXT`             | `TEXT`             | None; preserved, subject to live-schema check |
| `branches.name`         | `name`           | `TEXT NULL`        | `TEXT NULL`        | None; preserve if present in source contract  |
| `branches.code`         | `code`           | `TEXT NULL`        | `TEXT NULL`        | None; preserved                               |
| `customers.id`          | `id`             | `UUID`             | `UUID`             | None; preserved                               |
| `orders.id`             | `id`             | `UUID`             | `UUID`             | None; preserved                               |
| `orders.branch_id`      | `branch_id`      | `UUID NULL`        | `UUID NULL`        | None; preserved                               |
| `orders.customer_id`    | `customer_id`    | `UUID NULL`        | `UUID NULL`        | None; preserved                               |
| `orders.created_at`     | `created_at`     | `TIMESTAMPTZ NULL` | `TIMESTAMPTZ NULL` | None; preserved                               |
| `orders.status`         | `status`         | `TEXT`             | `TEXT`             | None; preserved                               |
| `orders.weight_kg`      | `weight_kg`      | `NUMERIC`          | `NUMERIC`          | None; preserved                               |
| `orders.total_price`    | `total_price`    | `NUMERIC`          | `NUMERIC`          | None; preserved                               |
| `payments.id`           | `id`             | `UUID`             | `UUID`             | None; preserved                               |
| `payments.order_id`     | `order_id`       | `UUID`             | `UUID`             | None; preserved                               |
| `payments.branch_id`    | `branch_id`      | `UUID NULL`        | `UUID NULL`        | None; preserved                               |
| `payments.amount`       | `amount`         | `NUMERIC`          | `NUMERIC`          | None; preserved                               |
| `payments.paid_at`      | `paid_at`        | `TIMESTAMPTZ`      | `TIMESTAMPTZ`      | None; preserved                               |
| `payments.source`       | `source`         | `TEXT`             | `TEXT`             | None; preserved                               |
| `expenses.id`           | `id`             | `UUID`             | `UUID`             | None; preserved                               |
| `expenses.branch_id`    | `branch_id`      | `UUID NULL`        | `UUID NULL`        | None; preserved                               |
| `expenses.amount`       | `amount`         | `NUMERIC`          | `NUMERIC`          | None; preserved                               |
| `expenses.expense_date` | `expense_date`   | `DATE`             | `DATE`             | None; preserved                               |
| `expenses.deleted_at`   | `deleted_at`     | `TIMESTAMPTZ NULL` | `TIMESTAMPTZ NULL` | None; preserved                               |

### Schema consistency and drift handling

Compare the live catalog with the approved contract before each run. Ignore unapproved extra columns. Stop on a missing, renamed, or incompatible required column. Send nullability or relationship changes for owner review. Update and version this contract before resuming; do not guess casts, defaults, or replacement fields.

## 7. Extraction Acceptance and Handover Rules

| Overall condition                                                                                                                                                                 | Acceptance decision | Pipeline action                                                                                                                | Decision owner                                  |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------- |
| All critical checks pass, counts match, and there are no unresolved date/reference warnings                                                                                       | **APPROVED**        | Mark complete and hand over the five raw datasets with the manifest, run ID, counts, checksums, and validation summary.        | Automatic                                       |
| Critical checks pass but nullable/missing dates or relationships, or a non-critical schema/count warning, need review                                                             | **MANUAL REVIEW**   | Hold from automatic transformation. Keep the source rows and evidence; resume after an authorized reviewer records a decision. | ETL/data administrator or designated data owner |
| Source is inaccessible; a required table/column is missing or incompatible; IDs are missing/duplicated; counts mismatch; snapshot is inconsistent; or output is truncated/corrupt | **REJECTED**        | Stop the run and withhold partial data. Log the failure and retry after the cause is resolved.                                 | Automatic stop; operator resolves               |

### Automatic approval

Approve automatically only when all five tables pass critical checks, row counts match, IDs are present and unique, the package comes from one complete snapshot, and no warning blocks branch/date assignment.

### Manual review

Require manual review for missing/invalid dates, NULL branch/customer references, compatible but unexpected schema changes, or unusual count changes. A non-NULL foreign key that has no matching source row is a critical failure. Record the check, affected table/count, decision, reason, and review time. Review does not authorize editing the extract; correct the source or document handling in a later stage.

### Rejection, retry, and quarantine

On a critical failure, mark the run `FAIL` and keep partial outputs isolated from accepted packages. Limit access to operators and apply the retention rules. After fixing the cause, start a new run with a new ID. Retry transient connection/time-out errors a limited number of times; remediate schema or contract failures before retrying. Never skip a table or mark a partial package complete.

### Handover package

Pass the following to Transformation:

1. One complete raw dataset for each of the five source tables.
2. A manifest containing pipeline/run ID, snapshot and extraction timestamps, source table names, output format/location, source and output row counts, checksums, and the full extraction window (`all available history` for this full-extract design).
3. Per-table and overall validation results, warnings, rejected count, and sanitized errors.
4. The approved schema-contract version and branch-label preflight result.

The receiving stage acknowledges the run ID and package status. It applies business filters, local-date derivation, cleaning, standardization, and aggregation; this extraction stage does none of those tasks.

## 8. Common Extraction Problems and Mitigation

| Problem                                           | Possible cause                                                                                                   | Impact                                                                                            | Proposed mitigation                                                                                                                                                                                         |
| ------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Supabase/PostgreSQL connection failure or timeout | Network outage, expired credentials, wrong connection string, database unavailable, or excessive concurrent load | No complete source snapshot; downstream data would be stale or incomplete.                        | Use a managed secret store and dedicated read-only identity; use bounded exponential retries for transient failures; alert on final failure; do not mark partial output approved.                           |
| Required schema drift                             | Migration changes a column/table/type, or the supplied schema document is stale                                  | Query failure or silent omission/misinterpretation of a field such as branch label or event date. | Run catalog preflight; fail on missing/incompatible required fields; review migration/schema diff, update and version this contract, then rerun with a new ID.                                              |
| Source read permission revoked                    | Role/grant/policy changed or credentials rotated incorrectly                                                     | One or more tables cannot be read, making the snapshot incomplete.                                | Probe all five tables before extraction; grant only required read access; alert the database owner; do not skip the failing table.                                                                          |
| Extraction interrupted after some tables          | Worker restart, network reset, storage error, timeout, or deployment                                             | Package contains only part of the source snapshot.                                                | Stage output under a non-accepted run ID; record table completion independently; mark run failed; resume by starting a new full run after checking source health.                                           |
| Source/output row counts differ                   | Truncated transfer, serialization bug, wrong query scope, or concurrent inconsistent reads                       | Records may be missing or duplicated.                                                             | Use one snapshot, count each source query and output, compare counts and checksums, and reject any mismatch.                                                                                                |
| Duplicate or missing primary identifiers          | Source integrity issue or duplicate output write                                                                 | Records cannot be reliably traced or referenced downstream.                                       | Test `id` nullability and uniqueness for every dataset; retain diagnostics and stop approval until the source/package is corrected.                                                                         |
| Null or unmatched branch/date references          | Legacy records, incomplete historical migration, or invalid manual/source writes                                 | Later branch/date grouping may omit records or assign them incorrectly.                           | Preserve rows and nulls; flag counts in validation; hold for manual review. Resolve at source or document downstream handling; never infer a branch/date during extraction.                                 |
| Late payment or historical correction             | Collection recorded after the order date, or an authorized source correction                                     | Incremental extraction could miss changed history and reports would diverge.                      | Current design performs a full extract; reconcile counts each run. If volume later requires incremental mode, define a payment `paid_at`/change cursor and a correction/lookback strategy before switching. |
| Numeric precision or NULL corruption in CSV       | Floating-point conversion or ambiguous empty-field/null handling                                                 | Payment/order/expense totals may change or missing values may become empty strings.               | Serialize PostgreSQL `NUMERIC` as exact decimal text; document and test CSV NULL/quote rules; validate round-trip values and reject lossy output.                                                           |
| Scheduler runs in wrong timezone                  | Cron configured in UTC while schedule is expressed as Manila time                                                | Extraction starts at an unintended local time or overlaps business activity.                      | Verify scheduler timezone and run history; use `0 17 * * *` only when scheduler is UTC; store timestamps with timezone and alert on missed runs.                                                            |
| Full-extract duration or load grows too large     | Historical tables grow or the job overlaps operational peaks                                                     | Slower branch transactions, missed schedule, or resource exhaustion.                              | Monitor duration, row counts, and source load; schedule off-peak; tune read-only queries/indexes with the DBA; only redesign extraction mode through a reviewed, versioned cursor/correction policy.        |

## Expected Output

The stage produces five raw table extracts and a manifest containing run details, row counts, checksums, validation results, and the approval status. It preserves source values and row grain for Transformation; it does not clean or standardize records, apply business filters, or calculate reporting metrics.
