# I&C Laundry ETL Pipeline — Stage 1: Data Extraction

**Status:** Extraction-stage specification; proposed, not evidence that an ETL job is deployed.

**Basis:** The source scope and business context follow _I-and-C-Laundry-ETL-Technical-Metadata-Branch-Only_. That document states its schema was owner-supplied and not directly inspected. Confirm every table and column against the live Supabase database before implementation.

**Boundary:** This document covers extraction, extraction-level validation, run logging, and handover only. Cleaning, standardization, business calculations, and reporting-table loading belong to later stages.

## 1. Data Source and Extraction Specification

### Source system

The source is the I&C Laundry operations system used by the Main, Calzada, and Nasugbu branches. Its operational purpose is to record customers, laundry orders and statuses, payment collections, expenses, and branch references in one Supabase PostgreSQL database.

This names the source for the extraction stage; it does not assert that the proposed ETL pipeline or its destination is already deployed.

### Source database

- **DBMS:** PostgreSQL hosted through Supabase.
- **Source schema:** `public`.
- **Source tables:** `branches`, `customers`, `orders`, `payments`, and `expenses`.
- **Access assumption:** An authorized ETL identity can read the required source tables. Use a dedicated, least-privilege read-only identity; do not place a database service-role secret in an end-user client.

### Extraction method

- **Mode:** Full extraction of all rows and required columns from the five in-scope tables on every scheduled run.
- **Reason:** The source specification requires completed-day history to be rebuildable so late collections and corrections are not missed. A full snapshot avoids a watermark that could silently omit those changes. Reassess this choice if data volume makes it impractical; any later incremental design must define a reliable cursor and correction/lookback policy.
- **Consistency:** Read the tables from one consistent PostgreSQL transaction snapshot. Record the snapshot/run start time. Preserve each source row at its source grain; do not join transaction tables together during extraction.
- **Mechanism:** Read-only SQL `SELECT` statements using an authorized PostgreSQL/Supabase connection. Extract the declared columns only. Package each table as a separate raw relational extract (CSV serialization is acceptable if the implementation records its format and preserves NULL, exact numeric, UUID, and timestamp values unambiguously).
- **Filtering:** Do not remove cancelled orders, soft-deleted expenses, null dates, or null branch IDs in this stage. Extract them and expose them to validation/handover; later stages own business inclusion rules and transformation.
- **Incremental cursor:** Not applicable to the specified full extraction. No high-water mark is used.
- **Schedule:** Daily at **01:00 Asia/Manila (UTC+08:00)**, as proposed in the technical metadata. If the scheduler uses UTC, the corresponding cron time is `17:00 UTC` on the previous UTC date (`0 17 * * *`). Verify the actual scheduler timezone before enabling it.
- **Business-date cutoff:** Extraction is a source snapshot, not a business-date filter. The downstream stage may process completed local dates earlier than the local run date; extraction must retain timestamps and dates unchanged so that stage can apply its documented cutoff.

### Extraction scope

Extract all available historical rows and only the columns listed in Section 2. The intended scope is branch-level reporting of order activity/value, payment collections, expenses, order weight/status, and distinct customer activity. Customer contact information is out of scope; only the customer identifier is needed for the declared reporting measures.

No source table is to be exported wholesale. No unrelated tables, application secrets, customer names, phone numbers, or email addresses are part of this extraction contract.

### Source limitations and assumptions

1. The referenced technical metadata is based on a supplied schema, not a verified live-schema dump. Required table names, column names, SQL types, nullability, keys, foreign keys, and grants must pass the live-schema preflight before a run is accepted.
2. `branches.branch_name` appears in the metadata document. Project migrations also define/retain `branches.name` and synchronize it with `branch_name`. Confirm which columns exist and which is the canonical label in the deployed database; the extraction must not silently substitute one for the other.
3. `orders.branch_id`, `orders.customer_id`, `orders.created_at`, `payments.branch_id`, and `expenses.branch_id` may be nullable in the supplied schema. Extraction preserves nulls; validation records them. Rows that cannot be assigned to a branch/date may need manual review before downstream processing.
4. A single database snapshot provides a consistent view of the selected tables, but it cannot prove the business records are complete or correctly entered.
5. Historical rows may have been backfilled or corrected. Preserve source dates and values exactly; the extraction stage must not infer missing collection dates or rewrite historical records.
6. Full extracts increase read and transfer volume as history grows. Monitor runtime and database load; do not switch to incremental extraction without documenting how late-arriving and corrected data will be captured.
7. The daily schedule and access grants are proposed settings. They are not confirmed as configured in Supabase Cron/pg_cron.

## 2. Source Tables and Column Specification

Types and relationships below follow the supplied ETL metadata. `NULL` means nullable according to that source document; confirm against the deployed schema. Numeric source columns have no precision/scale asserted here.

| Source table       | Purpose in this extraction                    | Selected columns and source types                                                                                                                   | Key and relationships                                                                                                   | Reason for inclusion                                                                                                                                                     |
| ------------------ | --------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `public.branches`  | Branch reference data                         | `id UUID`; `branch_name TEXT`; `name TEXT NULL`; `code TEXT NULL`                                                                                   | `id` primary key. Referenced by branch IDs in operational records.                                                      | Preserve branch identity and source labels for later branch-level grouping. Confirm label-column availability/canonical status before extraction.                        |
| `public.customers` | Customer reference data                       | `id UUID`                                                                                                                                           | `id` primary key. Referenced by `orders.customer_id`.                                                                   | Enables distinct-customer reporting without exporting customer contact details.                                                                                          |
| `public.orders`    | Laundry transaction and service-status source | `id UUID`; `branch_id UUID NULL`; `customer_id UUID NULL`; `created_at TIMESTAMPTZ NULL`; `status TEXT`; `weight_kg NUMERIC`; `total_price NUMERIC` | `id` primary key; `branch_id → branches.id`; `customer_id → customers.id`. One order can have multiple payment entries. | Supports order counts, released-order counts, customer activity, weight/demand, and order value. Keep cancelled and other statuses in the extract for later-stage rules. |
| `public.payments`  | Payment collection ledger                     | `id UUID`; `order_id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `paid_at TIMESTAMPTZ`; `source TEXT`                                           | `id` primary key; `order_id → orders.id`; `branch_id → branches.id`. One order can have many payment rows.              | Supports payment-entry counts and cash received on the collection date; retaining ledger row grain prevents deposits/installments from being confused with order counts. |
| `public.expenses`  | Branch expense records                        | `id UUID`; `branch_id UUID NULL`; `amount NUMERIC`; `expense_date DATE`; `deleted_at TIMESTAMPTZ NULL`                                              | `id` primary key; `branch_id → branches.id`.                                                                            | Supports branch expense reporting. Extract soft-deleted rows too; a later stage applies the documented non-deleted expense rule.                                         |

### Extraction row-grain rules

- One output row represents exactly one source row from the same-named source table.
- Do not join `orders` directly to `payments` or `expenses` during extraction; one-to-many relationships can duplicate source rows.
- Preserve source identifiers, source column names, values, NULLs, and statuses. Any future alias or type conversion must be added to the data contract and treated as a documented mapping.

## 3. Extraction Validation and Data Quality Checks

These are extraction checks only. They determine whether the required source snapshot was read and packaged correctly; they do not clean records or calculate business metrics.

| Check name                        | Target                                                                                                                               | Purpose                                                                  | Pass/fail criteria                                                                                                                                                                                                                                                                                            |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Source connection and read access | Supabase PostgreSQL connection and all five source tables                                                                            | Confirm the job can reach the expected database and read the source.     | **PASS:** connection succeeds and a read-only metadata/read probe succeeds for each table. **FAIL:** timeout, authentication/permission error, wrong database, or inaccessible table.                                                                                                                         |
| Required schema present           | The five tables and every selected column in Section 2                                                                               | Detect schema drift before reading data.                                 | **PASS:** every required table/column exists with compatible PostgreSQL type and expected key. **FAIL:** any required table/column is absent or incompatible. Branch-label variants are recorded; do not fail solely for an optional label, but fail if no agreed label source is available for the contract. |
| Source identifiers present        | `id` on all five tables; `orders.id`, `customers.id`, and `branches.id`                                                              | Ensure rows have stable identifiers for traceability and joins.          | **PASS:** zero NULL primary IDs in extracted rows. **FAIL:** one or more missing source primary IDs.                                                                                                                                                                                                          |
| Primary-key uniqueness            | Each extracted table's `id`                                                                                                          | Detect duplicate source rows/keys or packaging duplication.              | **PASS:** distinct `id` count equals extracted row count for each table. **FAIL:** mismatch.                                                                                                                                                                                                                  |
| Extraction completeness           | Each source table and its output dataset                                                                                             | Confirm all rows visible in the run snapshot were emitted.               | **PASS:** source count equals output count per table, and source/output counts are recorded. **FAIL:** mismatch, missing dataset, truncated output, or interrupted serialization.                                                                                                                             |
| Relationship coverage             | `orders.branch_id`, `orders.customer_id`, `payments.order_id`, `payments.branch_id`, `expenses.branch_id` against reference extracts | Surface unresolved references without rewriting or dropping rows.        | **PASS:** every non-NULL reference matches an extracted key. **WARNING:** NULL or unmatched nullable business reference is present; retain row and require review before downstream use. A non-null FK mismatch is **FAIL**.                                                                                  |
| Reporting date availability       | `orders.created_at`, `payments.paid_at`, `expenses.expense_date`                                                                     | Identify records that cannot be assigned to a reporting date downstream. | **PASS:** zero null/invalid required event dates. **WARNING:** any missing/invalid event date; preserve row and require review. Do not impute a date in extraction.                                                                                                                                           |
| Numeric serialization fidelity    | `orders.weight_kg`, `orders.total_price`, `payments.amount`, `expenses.amount`                                                       | Prevent precision loss or malformed values during export.                | **PASS:** exported value parses as a PostgreSQL-compatible numeric and round-trips without rounding. **FAIL:** parse error, overflow, or value changed by serialization. No business range checks are performed here.                                                                                         |
| Extraction interruption           | Run state and per-table output                                                                                                       | Detect partial runs.                                                     | **PASS:** all table reads, writes, and validation finish and the package is marked complete. **FAIL:** any query/serialization/upload fails or the run ends without finalization; partial package is not eligible for handover.                                                                               |
| Snapshot identity                 | Run metadata                                                                                                                         | Ensure all tables belong to one documented source snapshot.              | **PASS:** one transaction snapshot/run ID applies to all tables. **FAIL:** tables were read across inconsistent snapshots or snapshot metadata is absent.                                                                                                                                                     |

Counts and warnings are reported per table. Warnings do not authorize dropping or altering source rows.

## 4. Extraction Metadata and Log Specification

Write one run record and one table-level record per extracted table. Store logs separately from the raw extract payload; never log credentials or customer contact information.

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

Use run and table logs to monitor schedule success, compare source/output counts, identify schema or permission changes, diagnose interrupted reads, and determine whether a failed run can be safely retried. The `pipeline_run_id` and snapshot metadata link a handover package to its validation evidence. A full extraction is repeatable; retries must use a new run ID and must not publish an incomplete earlier package as accepted.

## 5. Log Retention and Access

- **Operational run logs:** Retain for at least **1 year** to cover routine monitoring, delayed issue discovery, and recovery. Extend retention when an incident, audit, or recovery is open.
- **Audit/recovery hold:** Do not delete any run log or associated manifest/package while needed for troubleshooting, audit, regulatory review, or recovery. Apply a documented hold and release it only after the responsible administrator confirms the need has ended.
- **Storage location:** A restricted ETL operations/log store associated with the pipeline environment. The exact product/location is an implementation decision and must be recorded before deployment. Do not put log files in a public bucket or frontend assets.
- **Access:** ETL service identity may create run records and write extracts. Pipeline operators/administrators may read logs and validation summaries. Source read credentials are restricted to the job runtime and authorized database administrators. Ordinary application users do not receive unrestricted ETL log or extract access.
- **Archiving:** Before expiry, archive only if needed for an approved audit/recovery purpose; preserve run ID, timestamps, checks, counts, and checksums. Encrypt archives at rest and restrict access equivalently.
- **Deletion:** After the retention period and when no hold exists, an authorized pipeline/database administrator may delete logs through the approved retention process. Record the deletion date, scope, and responsible role. Never delete logs early solely to make space; alert and expand/rotate storage instead.
- **Data minimization:** Run logs contain metadata, counts, checks, and sanitized errors, not customer names, phone numbers, email addresses, passwords, or credentials.

Operational logs support job operation. If the organization has separate legal/audit records with a longer retention requirement, those records must follow that policy and must not inherit the one-year operational-log period automatically.

## 6. Extraction Data Contract and Naming Convention

### Required schema and naming

- Preserve the source table names and selected source column names exactly in the extraction datasets.
- Use PostgreSQL-compatible types as declared in Section 2: `UUID`, `TEXT`, `TIMESTAMPTZ`, `DATE`, and `NUMERIC`.
- Preserve `NUMERIC` values without floating-point conversion or rounding. Preserve timestamps with their timezone/offset semantics; do not convert business dates in extraction.
- Preserve NULL distinctly from an empty string when serializing. Record CSV NULL/quoting rules or the equivalent format-specific rules in the run manifest.
- Preserve each source primary key (`id`) and all selected foreign-key fields. Do not create replacement identifiers.
- Table and column names are lowercase `snake_case`; do not abbreviate or rename source columns in this stage.
- Branch labels are reference values, not keys. Use branch UUIDs for traceability. The branch label field must follow the preflight-confirmed source contract (`branch_name`, `name`, or a documented source mapping); do not silently pick a different label.

### Source-to-extract field mapping

The Stage 1 contract is a raw extract: names and types are unchanged. The mapping below documents this explicitly; it is not a business transformation.

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

Before extraction, compare live catalog metadata with the approved contract. An added source column is ignored unless approved; a removed, renamed, or type-incompatible required column fails preflight and stops the run. A nullability or relationship change is at least a warning and requires owner review before acceptance. Update this contract and version the mapping before resuming. Do not guess a cast, rename, default, or replacement field in the extraction job.

## 7. Extraction Acceptance and Handover Rules

| Overall condition                                                                                                                                                                        | Acceptance decision | Pipeline action                                                                                                                                        | Decision owner                                  |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------- |
| Connection, schema, identifier, uniqueness, snapshot, count, serialization, and all other critical checks pass; no unresolved date/reference warnings                                    | **APPROVED**        | Mark package complete and pass the five raw datasets, manifest, run ID, counts, checksums, and validation summary to Transformation.                   | Automatic                                       |
| Critical checks pass, but nullable/missing dates or branch/customer/order relationships are found, or a non-critical count/schema warning is present                                     | **MANUAL REVIEW**   | Hold package from automatic transformation; retain source rows and evidence. Resume only after an authorized reviewer records a resolution/acceptance. | ETL/data administrator or designated data owner |
| Source is inaccessible, required table/column is missing/incompatible, primary IDs are missing/duplicated, counts mismatch, snapshot consistency is lost, or output is truncated/corrupt | **REJECTED**        | Stop run; do not hand over partial data. Record failure, retain safe diagnostics, and retry only after the cause is resolved.                          | Automatic stop; operator resolves               |

### Automatic approval

Automatic approval requires all critical checks to pass for all five tables, source/output counts to match, zero duplicate/missing primary keys, a complete single-snapshot package, and no unresolved warning that blocks branch/date assignment.

### Manual review

Manual review is required for missing/invalid reporting dates, null or unmatched branch references, unmatched nullable customer references, unexpected but compatible source changes, or unusual record-count changes. The reviewer records the check, affected table/count, decision, rationale, and timestamp. Manual approval does not permit edits to the extracted rows; corrections belong at the source or in a later documented stage.

### Rejection, retry, and quarantine

On a critical failure, stop publication and mark the run `FAIL`. Keep partial outputs isolated from accepted packages, with access limited to operators; delete them only under the retention/deletion process after recovery needs end. Correct the connection/schema/source issue and start a new run with a new ID. Use bounded retries for transient connection/time-out errors; do not retry persistent schema or data-contract failures without remediation. Never silently skip a source table or publish partial data as complete.

### Handover package

Pass to Transformation:

1. One complete raw dataset for each of the five source tables.
2. A manifest containing pipeline/run ID, snapshot and extraction timestamps, source table names, output format/location, source and output row counts, checksums, and the full extraction window (`all available history` for this full-extract design).
3. Per-table and overall validation results, warnings, rejected count, and sanitized errors.
4. The approved schema-contract version and branch-label preflight result.

The receiving stage must acknowledge the run ID and package status. Transformation decides business filters, local-date derivation, cleaning, standardization, and aggregation; none of those operations are performed by this extraction specification.

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

A complete, traceable, validated raw extract package for the five in-scope source tables, accompanied by run metadata, per-table counts/checksums, validation outcomes, and an explicit approval status. The package preserves source values and grain for handover to Transformation; it does not clean, standardize, filter business records, or calculate reporting metrics.
