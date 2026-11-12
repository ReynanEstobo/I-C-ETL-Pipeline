# I&C Laundry Daily Branch Reporting ETL

## Stage 3: Data Load

### Objective

Load the validated daily branch summary handed over by the Transformation Stage into the permanent reporting table, then confirm that the published data is complete, accurate, traceable, and ready for branch dashboards and reports. Load publishes the received rows unchanged. Re-extraction, cleaning, standardization, and business calculations are outside this stage.

This pipeline is batch only. Each daily run loads one bounded batch: the complete completed-day history up to the run's reporting cutoff. Streaming, micro-batch, and real-time loading are out of scope; live order tracking continues to use the operational tables.

## 1. Load Source and Target Specification

### Load input

Load receives `pg_temp.transformed_daily_branch_summary`, defined in the [Transformation data contract](../Transform/transformation.md#6-transformed-data-contract-and-naming-convention), through the [Transformation handover](../Transform/transformation.md#10-transformation-acceptance-and-handover-rules). The input is a PostgreSQL temporary table in the same database connection used by extraction and transformation. No file export or external staging store is used.

| Handover item | Use in Load |
| --- | --- |
| `pg_temp.transformed_daily_branch_summary` | Rows to publish: one row per `report_date` and `branch_id`. |
| Transformation `run_id` | Acknowledged before receipt; stored as `transformation_run_id`. |
| Parent extraction run ID (`parent_run_id`, the Stage 1 `pipeline_run_id`) and `extraction_started_at`, passed through unchanged by Stage 2 | Lineage to Stage 1 and the snapshot-freshness check. |
| `batch_id`, `batch_window` (built by the handover when Stage 2 sends only `cutoff`), and reporting cutoff | Identify the completed-day history being published. |
| `records_output`, `quarantined_records`, `validation_results`, `code_version`, `rule_version`, and `schema_version` | Pre-load completeness checks and lineage. |
| `acceptance_status` | Must be `APPROVED` or `APPROVED_WITH_QUARANTINE`. |

The batch window runs from its start date up to, but excluding, the reporting cutoff. The cutoff is the Asia/Manila calendar date on which the extraction run started. Stage 2 sends the cutoff as `cutoff` but sends no `batch_window`, so the Stage 2-to-Load handover builds one: `start` is the earliest `report_date` in the transformed output, and `cutoff` is the Asia/Manila date of `extraction_started_at`. A `batch_window` sent by Stage 2 is passed through unchanged. A date inside the window with no eligible activity has no output row. Load reads the window as text such as `2026-10-12 to 2026-11-12`, a date range such as `[2026-10-12,2026-11-12)`, or a structured value with start and cutoff dates. For example, a run starting at 2026-11-12 01:00 Asia/Manila has cutoff 2026-11-12 and publishes dates up to 2026-11-11.

Load must use only the handed-over rows. It must not read `public` source tables or `pg_temp.stage_*` tables to calculate or replace values, and it must not recalculate measures. The only reads outside the received batch are target-side checks: the published target, the publication records and removal approvals in `etl_ops`, and the branch foreign key.

### Target system

| Item | Specification |
| --- | --- |
| Target | `analytics.daily_branch_summary`, the permanent daily branch reporting table. |
| Purpose | Reusable completed-day measures for sales monitoring, payment collections, expenses, laundry demand, branch comparison, and customer activity. |
| Database management system | PostgreSQL in the existing Supabase project, the same database that holds the operational records. |
| Target schema | `analytics` for reporting data. Load metadata is stored in the restricted `etl_ops` schema. |
| Consumers | The application's authorized reporting layer, with SELECT access only. |

### Load method

- **Tools:** SQL inside PostgreSQL writes the target. Python wraps the load for manual runs and testing using `psycopg`. Records must stay in PostgreSQL rather than being downloaded into Python memory.
  - `python -m pipeline.load` runs extraction, transformation, and load on one connection and prints a short summary (Section 8). `--verbose` also prints every detail as it happens. Each run's full output is saved in `logs/etl-run-<date>-<time>.log`.
  - `python -m pipeline.load test` runs the load scenarios inside one transaction that is rolled back, so nothing is published or logged.
  - `python -m pipeline.load --flush-logs` delivers queued load metadata.
- **Schedule:** Daily at **01:00 Asia/Manila** (`0 17 * * *` when the scheduler uses UTC), immediately after approved transformation in the same run. Load has no separate Cron job, because its input exists only in the run's session. The Supabase Cron / `pg_cron` job `ic_laundry_daily_extraction` runs `CALL etl_ops.run_scheduled_pipeline();`, which runs `etl_ops.run_scheduled_extraction()` and then publishes in the same session.
- **Write method:** Set-based bulk SQL from the received temporary table: one `INSERT ... ON CONFLICT (report_date, branch_id) DO UPDATE` statement, plus a `DELETE` limited to approved absent keys (Section 3). Rows are not inserted one at a time, and `TRUNCATE` is not used.

### Transaction design

The extraction snapshot transaction is read-only `REPEATABLE READ`. PostgreSQL allows writes to temporary tables in a read-only transaction but rejects writes to permanent tables. Load therefore receives the batch inside the snapshot transaction and publishes it in a separate read-write transaction, in the same session:

| Step | Transaction | Load work |
| --- | --- | --- |
| 1. Setup | Existing read-write setup transaction, before the snapshot | Create an empty `pg_temp.load_daily_branch_summary` with `ON COMMIT PRESERVE ROWS`, matching the ten Stage 2 contract columns. No permanent writes. |
| 2. Receipt | Existing read-only `REPEATABLE READ` snapshot transaction | Acknowledge the transformation run ID and acceptance status. Copy the offered rows unchanged into `pg_temp.load_daily_branch_summary`. Capture the received count and per-measure totals. Do not commit or close the connection. |
| 3. Publication | New read-write `READ COMMITTED` transaction after the snapshot commits | Take the load lock, run pre-load checks, write the target and its `etl_ops.load_publications` record, run post-load checks, then `COMMIT` only if every critical check passes. |
| 4. Logging | Separate logging transaction | Persist load metadata, then clear `pg_temp.load_daily_branch_summary`. |

The snapshot transaction stays read-only, so source tables cannot change during extraction. The load-owned copy keeps the received batch available after extraction staging is cleared at commit. The publication transaction reads only that copy, the target, and the `etl_ops` load tables. Because the session default remains read-only, the publication transaction must explicitly switch to read-write: `SET TRANSACTION READ WRITE` in the scheduled procedure, or a read-write transaction on the `psycopg` connection.

In scheduled runs, `etl_ops.consume_transformation_with_load(jsonb)` is the consume function in `etl_ops.extraction_handover`. It runs the Stage 2 consumer, builds the batch window when Stage 2 sends none, then calls the Load receiver configured by the database administrator in `etl_ops.transformation_handover`, which has the same structure as `etl_ops.extraction_handover`. The receiver functions, `etl_ops.load_acknowledge(jsonb)` and `etl_ops.load_receive(jsonb)`, use invoker permissions inside the snapshot transaction. After the extraction procedure returns, `etl_ops.run_scheduled_pipeline()` runs `etl_ops.publish_daily_branch_summary(load_run_id, retry_of_run_id)` in the publication transaction. Manual runs pass a Load receiver to the Stage 2 consumer, which builds the batch window the same way, and run publication on the same connection before it closes. With no receiver configured, Stage 2 records the handover as `NOT_CONFIGURED`, and nothing is published. `python -m pipeline.scheduling update-handover` installs the current scheduled handover function.

### Load scope

- **Tables and order:** One target table, `analytics.daily_branch_summary`. No dimension tables are loaded: `public.branches` is the existing branch reference, maintained by the application. The order is receipt, pre-load checks, target write and publication record, post-load checks, `COMMIT`, then logging.
- **Expected volume:** One row per completed business date and branch with eligible activity: at most 3 × the number of completed dates, or about 1,095 rows for each year of history. The profiled Stage 2 batch covered two business dates, so at most 6 rows.
- **Time window:** The publication transaction uses `SET LOCAL lock_timeout = '5s'`. Manual runs also use `SET LOCAL statement_timeout = '60s'`; scheduled runs do not, because the whole Cron procedure runs as one statement. The whole run, including retries, must finish before 02:00 Asia/Manila, well before branches open.

### Limitations and assumptions

- The received batch exists only in the run's session. If the connection closes before publication, the batch cannot be published; the previous publication remains, and a new run must start from extraction.
- Each run replaces the complete completed-day history. The workload grows with history, roughly 1,000 rows per year, which is small for PostgreSQL. Reassess before history spans many years or more branches are added.
- Released orders are grouped by creation date (Stage 2, B-04). A later status change can revise an earlier published date. This is an expected update, not an error.
- `unique_customer_count` is distinct within one date and branch. Adding it across dates or branches overcounts customers, so reports must not sum it.
- **Pipeline account:** Load runs in the same session as extraction and transformation, so one database account runs the whole pipeline. Manual runs connect as the project's `postgres` account, and the Cron job `ic_laundry_daily_extraction` runs as `postgres`, the account that scheduled it.
  - `postgres` owns the source, `analytics`, and `etl_ops` tables and has the `BYPASSRLS` attribute, so it reads every source row even though row-level security is enabled on the source tables.
  - The read-only snapshot transaction keeps extraction, transformation, and receipt from writing permanent tables. Only the publication transaction writes, and only to `analytics.daily_branch_summary` and the `etl_ops` load tables.
  - The write-permission check in Section 4 confirms the account's privileges before every publication.
- **Planned least-privilege role:** `postgres` can also change source data and schemas, which the pipeline never needs. Before production, the runs can move to a dedicated role, `ic_etl_runtime`, which is not created yet. It would hold:
  - **Source access:** SELECT on the required `public` source columns, plus the `BYPASSRLS` attribute or a SELECT policy on each of the five source tables, so it sees every source row.
  - **Session:** TEMPORARY on the database, and USAGE on the `analytics` and `etl_ops` schemas.
  - **Stage 1 and Stage 2:** EXECUTE on `etl_ops.run_scheduled_extraction()` and on the `etl_ops` extraction, transformation, and log-recovery functions. SELECT and INSERT on the extraction and transformation log, pending-log, and quarantine tables, and DELETE on their pending-log tables. SELECT on `etl_ops.extraction_handover` and `etl_ops.transformation_handover`.
  - **Load:** SELECT, INSERT, UPDATE, and DELETE on `analytics.daily_branch_summary`. SELECT and INSERT on `etl_ops.load_publications`, `etl_ops.load_logs`, `etl_ops.pending_load_logs`, and `etl_ops.load_rejections`, and DELETE on `etl_ops.pending_load_logs`. SELECT and UPDATE on `etl_ops.load_review_approvals`, to mark approvals as applied.
  - **Scheduling:** USAGE on the `cron` schema. The Cron job would be re-created while connected as `ic_etl_runtime`, because `pg_cron` runs each job as the role that scheduled it.
  - **Never granted:** TRUNCATE, DDL, or INSERT, UPDATE, and DELETE on `public` tables.
- When the runs move to `ic_etl_runtime`, and after any later row-level security change, compare the source row counts it sees with those seen by `postgres`. Extraction compares source and staging counts using the same role, so a role that cannot see some rows would pass that check while missing history.
- Passwords and connection strings stay in the local `.env` file or Supabase secrets, never in documentation or logs.
- A database administrator creates the target, indexes, and load metadata tables once by running the load setup SQL in the Supabase SQL Editor. The scheduled-load SQL then adds the scheduled functions, configures `etl_ops.transformation_handover`, and sets the storage limit in `etl_ops.load_settings`. The daily load never runs DDL; it only checks that those objects match Section 2.
- Writes take `ROW EXCLUSIVE` locks, which do not block readers. Dashboards continue to read the previous publication until the publication transaction commits.

## 2. Target Tables and Column Specification

| Table | Type | Primary key | Foreign keys | Key constraints | Purpose |
| --- | --- | --- | --- | --- | --- |
| `analytics.daily_branch_summary` | Aggregate (daily summary fact) | Composite `(report_date, branch_id)`; no surrogate key | `branch_id` → `public.branches(id)` | Every column NOT NULL; non-negative measures; `released_order_count` and `unique_customer_count` never exceed `order_count`; `report_date < reporting_cutoff` | Completed-day branch measures for reporting |
| `etl_ops.load_publications` | Control (publication record) | `load_run_id` | None | `transformation_run_id` UNIQUE; every column NOT NULL | Committed publication history for the duplicate-batch and freshness checks |

| Column | Type | Constraints and default | Role in the load | Reason for inclusion |
| --- | --- | --- | --- | --- |
| `report_date` | DATE | NOT NULL; `CHECK (report_date < reporting_cutoff)` | Match key, part 1 | Business date of the activity |
| `branch_id` | UUID | NOT NULL; FK → `public.branches(id)` | Match key, part 2 | Branch grouping; labels come from `public.branches.branch_name` at query time |
| `order_count` | BIGINT | NOT NULL; `CHECK (order_count >= 0)` | Measure | Eligible order activity |
| `released_order_count` | BIGINT | NOT NULL; between 0 and `order_count` | Measure | Released orders, by creation date |
| `unique_customer_count` | BIGINT | NOT NULL; between 0 and `order_count` | Measure (not additive) | Daily customer activity |
| `total_weight_kg` | NUMERIC(14,2) | NOT NULL; `>= 0` | Measure | Laundry demand in kilograms |
| `order_value_php` | NUMERIC(14,2) | NOT NULL; `>= 0` | Measure | Order value on the order creation date |
| `payment_count` | BIGINT | NOT NULL; `>= 0` | Measure | Number of payment collections |
| `cash_received_php` | NUMERIC(14,2) | NOT NULL; `>= 0` | Measure | Cash received on the collection date |
| `expense_total_php` | NUMERIC(14,2) | NOT NULL; `>= 0` | Measure | Expenses on the recorded expense date |
| `reporting_cutoff` | DATE | NOT NULL | Publication metadata | Lets reports state "completed days through the day before `reporting_cutoff`" |
| `load_run_id` | UUID | NOT NULL | Batch ID column | Links every row to its load log, and through it to the transformation and extraction runs |
| `refreshed_at` | TIMESTAMPTZ | NOT NULL | Publication metadata | Publication transaction timestamp, shown on reports |

The ten measure and key columns match the Stage 2 output contract exactly in name and type. Load adds only `reporting_cutoff`, `load_run_id`, and `refreshed_at`. Measure columns have no default values, so a missing value fails the insert instead of silently becoming zero. The table has no SCD Type 2 history columns, because aggregates are overwritten (Section 3).

**Indexes and partitions:** The primary-key B-tree index on `(report_date, branch_id)` serves the match key and date-range queries. A secondary index on `(branch_id, report_date)` supports per-branch trends and foreign-key checks when a branch row changes. The table is not partitioned: it grows by about 1,100 rows per year.

### Publication control table

The load also writes one row to `etl_ops.load_publications` inside the publication transaction. Reports do not read it. It records each committed publication, so the duplicate-batch check, the freshness check, and commit verification never depend on log delivery.

| Column | Type | Constraints | Purpose |
| --- | --- | --- | --- |
| `load_run_id` | UUID | Primary key | Matches the `load_run_id` stamped on the published rows |
| `transformation_run_id` | UUID | NOT NULL; UNIQUE | Prevents the same transformation batch from being published twice |
| `extraction_run_id` | UUID | NOT NULL | Lineage to Stage 1 |
| `extraction_started_at` | TIMESTAMPTZ | NOT NULL | Snapshot time compared by the freshness check |
| `reporting_cutoff` | DATE | NOT NULL | Cutoff of the published batch |
| `published_at` | TIMESTAMPTZ | NOT NULL | Publication transaction timestamp, equal to `refreshed_at` |
| `records_published` | BIGINT | NOT NULL; `>= 0` | Number of rows written to the target |

Rows are only inserted, never updated. A rolled-back publication leaves no row.

## 3. Load Strategy and Rules

| Table | Load type | Write logic | Match key | On failure |
| --- | --- | --- | --- | --- |
| `analytics.daily_branch_summary` | Full: the complete completed-day history every run | Upsert: insert new keys, update existing keys; delete only approved absent keys | `(report_date, branch_id)` | Roll back the publication transaction, keep the previous publication, stop the batch |

### Load type

Every batch carries the complete completed-day history. After commit, the target contains exactly the batch rows. Load reaches this end state by synchronizing on the match key rather than truncating and reloading. The result is the same as a full reload, but it avoids an `ACCESS EXCLUSIVE` lock and makes inserts, updates, and removals measurable. Loading by date increment is not used, because late payments and status changes revise earlier dates.

### Write logic

1. **Insert:** Keys in the batch but not in the target are inserted.
2. **Update:** Existing keys are overwritten with the batch measures and the run's `reporting_cutoff`, `load_run_id`, and `refreshed_at`. A row counts toward `records_updated` when any measure changes, and toward `records_unchanged` when only the publication columns change.
3. **Absent keys:** Keys published earlier but missing from the batch are deleted only when approved under Section 8. Otherwise the batch is held for manual review.

After every committed load, all rows carry the same `load_run_id` and `refreshed_at`, so the table always represents exactly one publication.

### Handling of changing data

- **Overwrite:** Changes are overwritten (SCD Type 1). The previous measures of each revised or removed key are recorded in the load log's `revisions` field for audit. The target itself keeps no history rows.
- **Deleted or inactive records:** Stage 2 excludes cancelled orders and soft-deleted expenses (B-02, B-03). Load does not filter rows again. The target has no soft-delete flag: a key is either part of the current publication or removed with approval.
- **Late-arriving records:** These arrive in a later batch's full history on their own business date, appearing as an insert or an update. Records dated on or after the cutoff are deferred by Stage 2 and published by a later run.
- **Schema differences:** A handover whose schema differs from the contract is rejected, never loaded and flagged. An intentional contract change requires a versioned target migration and a matching Stage 2 `schema_version`.

### Load order and dependencies

- No dimension tables are loaded. The branch reference is the existing `public.branches` table, and the foreign key guarantees that every published `branch_id` exists there.
- A row with no matching branch rejects the batch; there is no Unknown member. Stage 2 already requires resolved branches, so a mismatch at load time means a branch was deleted after the snapshot.
- The publication record in `etl_ops.load_publications` is written inside the publication transaction. The full load metadata is written after that transaction ends.

### Transaction and rollback handling

- The whole batch is loaded in one publication transaction: pre-load checks, target writes, the publication record, and post-load checks.
- Any critical failure, SQL error, constraint violation, or timeout ends in `ROLLBACK`. The target returns to the previous publication with no partial rows. A held `MANUAL REVIEW` batch also ends in `ROLLBACK`.
- Failure evidence is captured before the rollback and persisted by the separate logging transaction, so it survives (Section 6).

### Rerun and idempotency

- The composite primary key prevents duplicate keys, and the upsert overwrites values instead of appending. Running the same batch twice cannot duplicate rows or inflate totals.
- **Duplicate batch check:** A transformation run that already has a row in `etl_ops.load_publications` is not published again. The UNIQUE constraint on `transformation_run_id` also enforces this in the database.
- **Freshness check:** Load never publishes a snapshot older than the one currently published. The extraction advisory lock is released when the snapshot commits, so two runs can reach publication in either order. The check reads `etl_ops.load_publications`, which commits together with the target, so it never depends on log delivery.
- **Retries:** Transient publication errors are serialization failure (`40001`), deadlock (`40P01`), lock not available (`55P03`), and a busy load lock. These are retried in the same session from the same received copy, up to 3 attempts in total, waiting 2 seconds and then 4 seconds, the same retry policy used by extraction. Each attempt gets a new `load_run_id`, linked by `retry_of_run_id`.
- **Connection loss** is not retried within the run, because the received copy is gone. The operator first verifies the commit outcome (Section 9). Then a manual run or the next scheduled run publishes from a new extraction snapshot. Recovery never reuses temporary input from a closed session.

## 4. Pre-Load Validation Checks

Checks at receipt run inside the snapshot transaction, before Load accepts the batch. Checks before writing run inside the publication transaction, before any target row changes.

### At receipt (snapshot transaction)

| Check name | Target | Purpose | Validation criteria | Severity |
| --- | --- | --- | --- | --- |
| Handover status | Stage 2 handover metadata | Load approved data only | The acknowledgment returns the offered `run_id` and `acceptance_status`, which is `APPROVED` or `APPROVED_WITH_QUARANTINE`. `MANUAL REVIEW` and `REJECTED` batches are never received. | Critical |
| Handover completeness | Stage 2 handover metadata | Keep every batch traceable | The transformation run ID, parent extraction run ID, `extraction_started_at`, `batch_id`, batch window, cutoff, `records_output`, validation results, and versions are present. `quarantined_records` is present for `APPROVED_WITH_QUARANTINE` and treated as 0 for `APPROVED`. The batch window cutoff equals the Asia/Manila date of `extraction_started_at`. | Critical |
| Input contract | `pg_temp.transformed_daily_branch_summary` | Prevent type and column errors | The ten contract columns exist with the Section 2 types, and the Stage 2 `schema_version` is supported (currently `1.0.0`). | Critical |
| Input record count | `pg_temp.load_daily_branch_summary` | Detect lost or extra rows during receipt | Rows copied = rows in the transformed output = handover `records_output`. | Critical |

### Before writing (publication transaction)

| Check name | Target | Purpose | Validation criteria | Severity |
| --- | --- | --- | --- | --- |
| Target connection and write mode | Publication transaction | Make sure the load can write | The session is open within the 10-second connect timeout, and `transaction_read_only = off`. | Critical |
| Write permissions | Load role | Prevent failures mid-load | `has_table_privilege` confirms SELECT, INSERT, UPDATE, and DELETE on the target; SELECT and INSERT on `etl_ops.load_publications`, the load logs, and `etl_ops.load_rejections`; and SELECT and UPDATE on `etl_ops.load_review_approvals`. | Critical |
| Target schema match | `analytics.daily_branch_summary` and `etl_ops.load_publications` | Prevent column and constraint errors | The target exists with the 13 Section 2 columns and types, the primary key, the foreign key, and the CHECK constraints. `etl_ops.load_publications` exists with its primary key and UNIQUE constraint. | Critical |
| Overlap lock | Load advisory lock | Prevent two simultaneous publications | `pg_try_advisory_xact_lock` succeeds on the dedicated load key, which is separate from the extraction and log-recovery keys. | Critical; retried as transient |
| Duplicate batch | `etl_ops.load_publications.transformation_run_id` | Avoid publishing twice | No committed publication exists for this transformation run ID. | Critical |
| Snapshot freshness | Latest row in `etl_ops.load_publications` | Prevent an older batch from replacing newer data | The incoming `extraction_started_at` is later than the latest published `extraction_started_at`, or no publication exists yet. | Critical; not retried |
| Storage capacity | Supabase database | Avoid running out of space mid-load | `pg_database_size(current_database())` plus the estimated batch size is below 95% of the plan limit, set in `DATABASE_SIZE_LIMIT_MB` for manual runs and in `etl_ops.load_settings.database_size_limit_mb` for scheduled runs. Between 80% and 95%, or when the limit is not set, is reported as a warning. | Critical above 95%; warning (reported only) at 80–95% or when the limit is not set |
| Published history removal | Target keys versus received keys | Detect history disappearing from reports | Every currently published key is in the batch, or it is approved for removal in `etl_ops.load_review_approvals`. | Warning; holds publication |

## 5. Post-Load Validation and Reconciliation

Post-load checks run after the target writes and before `COMMIT`, in the same publication transaction. Any critical failure rolls the publication back.

| Check name | Target | Purpose | Validation criteria | Severity |
| --- | --- | --- | --- | --- |
| Record reconciliation | Target and received copy | Detect lost or extra records | received = inserted + updated + unchanged + rejected. A committed load has rejected = 0. | Critical |
| Target row count | `analytics.daily_branch_summary` | Confirm that the target holds exactly this batch | Total target rows = rows carrying this `load_run_id` = received rows. | Critical |
| Key uniqueness | `(report_date, branch_id)` | Prevent duplicates | Duplicate key count = 0. | Critical |
| Referential integrity | `branch_id` | Prevent orphan rows | 100% of `branch_id` values match `public.branches.id`. | Critical |
| Required values | All 13 columns | Ensure data completeness | NULL count = 0. | Critical |
| Exact content match | The ten contract columns | Confirm values were published unchanged | Target `EXCEPT` received copy and received copy `EXCEPT` target both return 0 rows. | Critical |
| Measure reconciliation | Eight measure columns | Confirm no value was lost | Each target sum equals the exact NUMERIC total captured at receipt. | Critical |
| Single publication | `load_run_id`, `refreshed_at`, `reporting_cutoff` | Keep one consistent publication stamp | Exactly one distinct value each, equal to this run's values. | Critical |
| Publication window | `report_date` and `reporting_cutoff` | Publish only completed days inside the batch window | Every `report_date` is on or after the batch window start and before `reporting_cutoff`. When the handover builds the window, its start is the earliest published date, so the lower bound is always met. Dates with no eligible activity have no row, so the published dates can cover less than the full window. | Critical |
| Load errors and interruptions | Publication transaction | Catch failed writes | No SQL error, constraint violation, or timeout occurs before `COMMIT`. | Critical |
| Publication record | `etl_ops.load_publications` | Make the publication verifiable without the logs | Exactly one row exists for this `load_run_id`, with `records_published` equal to the received rows and `published_at` equal to `refreshed_at`. | Critical |

After `COMMIT`, the run confirms that the batch was published by reading its row in `etl_ops.load_publications`. If the commit result is unknown because the connection dropped, follow the commit-outcome mitigation in Section 9.

`records_rejected` counts received rows that the load did not write. In PostgreSQL, a constraint violation aborts the transaction, so Load never commits a subset of a batch. A committed batch therefore has `records_rejected = 0`. A failed batch records 0 rows loaded and `records_rejected = records_received`, with the failing keys and check stored in `etl_ops.load_rejections` when they can be identified. Rows quarantined, excluded, or deferred by Stage 2 never reach Load and are not counted here.

The sum of `unique_customer_count` is compared only as a checksum, never read as a total customer count. Post-load checks report problems without cleaning or correcting data; Section 8 decides the outcome.

## 6. Load Metadata and Log Specification

Store one row per load attempt in restricted `etl_ops.load_logs`.

| Field | Purpose | Example or format |
| --- | --- | --- |
| `load_run_id` | Unique ID of the load attempt | UUID |
| `transformation_run_id` | Link to the Stage 2 run | UUID |
| `extraction_run_id` | Link to the Stage 1 `pipeline_run_id` | UUID |
| `retry_of_run_id` | Previous load attempt for the same received batch | UUID or NULL |
| `batch_id` | Stage 2 logical batch identifier, copied unchanged | TEXT from the handover |
| `batch_window` | Batch window received with the handover: Stage 2's window, or the window the handover builds (earliest output date and exclusive cutoff) | `{"cutoff": "2026-11-12", "start": "2026-10-05"}` |
| `published_date_range` | First and last `report_date` actually published | `2026-10-12 to 2026-11-11` |
| `reporting_cutoff` | Exclusive completed-day cutoff | `2026-11-12` |
| `extraction_started_at` | Snapshot time, used by the freshness check | `2026-11-12 01:00:00+08:00` |
| `table_name` | Target table | `analytics.daily_branch_summary` |
| `load_type` | Full or incremental | `FULL` |
| `load_started_at`, `load_ended_at` | Publication boundaries | `2026-11-12 01:00:09+08:00`, `2026-11-12 01:00:10+08:00` |
| `duration_ms` | Load runtime | `850` |
| `input_acceptance_status` | Stage 2 decision received | `APPROVED` or `APPROVED_WITH_QUARANTINE` |
| `quarantined_input_records` | Stage 2 quarantine count, carried for visibility | `0` |
| `records_received` | Rows received from transformation | `62` |
| `records_inserted` | New keys written | `3` |
| `records_updated` | Existing keys whose measures changed | `2` |
| `records_unchanged` | Existing keys only restamped | `57` |
| `records_deleted` | Approved absent keys removed | `0` |
| `records_rejected` | Received rows not written | `0` |
| `measure_totals` | Per-measure totals captured at receipt and confirmed after writing | JSONB |
| `revisions` | Keys updated or removed, with previous and new measures | JSONB |
| `pre_load_results`, `post_load_results` | Check outcomes with expected and actual values | JSONB |
| `load_status` | Validation outcome | `PASS`, `WARNING`, `FAIL` |
| `acceptance_status` | Publication decision | `APPROVED`, `MANUAL REVIEW`, `REJECTED` |
| `log_delivery_status` | Log persistence, separate from the data decision | `PENDING`, `SAVED`, `FAILED` |
| `error_code` | Failure category | One of the error codes below, or NULL |
| `error_message` | Sanitized description without credentials | TEXT or NULL |
| `load_code_version`, `schema_version` | Load implementation and target contract versions | `1.0.0` |

### Error codes

| Error code | Meaning |
| --- | --- |
| `HANDOVER_NOT_APPROVED` | The Stage 2 acceptance status is not `APPROVED` or `APPROVED_WITH_QUARANTINE`. |
| `HANDOVER_INCOMPLETE` | Required handover metadata is missing, or the batch window cutoff does not match `extraction_started_at`. |
| `HANDOVER_MISMATCH` | The received rows do not belong to the acknowledged transformation run. |
| `SCHEMA_MISMATCH` | The Stage 2 output or the target does not match the contract. |
| `INPUT_COUNT_MISMATCH` | Rows copied, rows in the transformed output, and `records_output` differ. |
| `READ_ONLY_TRANSACTION` | The publication transaction is not read-write. |
| `PERMISSION_DENIED` | The pipeline role lacks a required privilege. |
| `LOAD_LOCK_BUSY` | Another load holds the publication lock. Retried. |
| `DUPLICATE_BATCH` | The transformation run is already published. |
| `STALE_BATCH` | A newer snapshot is already published. |
| `STORAGE_LIMIT` | Storage would exceed 95% of the plan limit. |
| `HISTORY_REMOVAL_HELD` | Published keys are absent from the batch and not approved for removal (`MANUAL REVIEW`). |
| `RECONCILIATION_FAILED` | One or more post-load checks failed. |
| `STATEMENT_TIMEOUT` | A statement exceeded the 60-second limit. |
| `FOREIGN_KEY_VIOLATION`, `CONSTRAINT_VIOLATION`, `DUPLICATE_KEY` | A database constraint rejected the write. |
| `TRANSIENT_DATABASE_ERROR` | Serialization failure, deadlock, or lock timeout. Retried. |
| `COMMIT_UNVERIFIED` | `COMMIT` returned, but no publication record was found. |
| `COMMIT_OUTCOME_UNKNOWN` | The connection failed during `COMMIT`; check `etl_ops.load_publications` before any rerun. |
| `LOG_QUEUE_FAILED` | Load metadata could not be queued before `COMMIT`, so the publication rolled back. |
| `SNAPSHOT_NOT_COMMITTED` | The batch was received, but the extraction snapshot did not commit; nothing was published. |
| `LOAD_FAILED` | Any other database error. |

### Use of load metadata

- **Monitoring:** Track the daily decision, duration, received count, and inserts, updates, and removals across runs.
- **Troubleshooting:** Read the expected and actual values of failed checks and the sanitized error.
- **Auditing:** Use `revisions` and approval references to explain why a published historical value changed.
- **Lineage:** Follow a published row's `load_run_id` to its load log, then to `transformation_run_id`, then to `extraction_run_id`.
- **Recovery:** Identify the last `APPROVED` publication, the retry chain, and whether an uncertain commit was published.

### Log delivery

Failure evidence must survive rollback. Capture diagnostics before the publication transaction ends, and persist them in a separate logging transaction afterwards. Before `COMMIT`, the wrapper queues the entry with an unverified outcome, then finalizes it once the commit result is known. If a run stops before finalizing, a later flush resolves the entry from `etl_ops.load_publications`. An unverified entry less than ten minutes old is left for a later flush, because its publication may still be running.

Manual runs keep undelivered entries in the private `.pending-extraction-logs` directory with a `.load` suffix, separate from the `.transformation` entries of Stage 2. Replay them with `python -m pipeline.load --flush-logs`. Scheduled runs write each entry to `etl_ops.pending_load_logs` in the same transaction as the publication, then `etl_ops.recover_load_logs()` delivers it to `etl_ops.load_logs`; undelivered entries are replayed at the start of the next scheduled run. Replay preserves the original IDs and timestamps, avoids duplicates using `load_run_id`, and removes a pending entry only after delivery commits.

The publication record in `etl_ops.load_publications` commits with the target, so the duplicate-batch check, the freshness check, and commit verification never wait for log delivery. Log delivery failure is separate from publication. Record it as `PENDING` or `FAILED`, and never republish a batch because its log failed.

## 7. Log Retention, Rejected Data, and Access

| Record | Storage | Retention | Contents |
| --- | --- | --- | --- |
| Load logs | `etl_ops.load_logs` | At least one year | Section 6 fields |
| Publication records | `etl_ops.load_publications` | At least one year; the latest row is kept while its batch is published | One row per committed publication: run IDs, `extraction_started_at`, cutoff, publication time, row count |
| Rejected and held keys | `etl_ops.load_rejections` | At least one year | `load_run_id`, `report_date`, `branch_id`, check name, reason, and the offending aggregate values. No customer or source records. |
| Removal approvals | `etl_ops.load_review_approvals` | At least one year after use | Approved keys, reason, approving role, approval time, and the `load_run_id` that applied it |
| Pending metadata | Private local pending directory; `etl_ops.pending_load_logs` | Until delivery is confirmed | Undelivered load metadata only |
| Published data | `analytics.daily_branch_summary` | Current publication only | Previous values of revised keys are kept in `revisions` |

Stage 1 and Stage 2 logs use the same one-year retention period. It is long enough to investigate historical report changes, failed batches, and approved removals. Longer organizational audit requirements take precedence, and approval and maintenance records follow that audit period separately from operational logs.

| Role | Read | Write | Delete |
| --- | --- | --- | --- |
| Pipeline account (`postgres` today; `ic_etl_runtime` once created) | Target, publication records, load logs, approvals, and the Stage 1 and Stage 2 objects listed in Section 1 | Target rows; inserts into publication records, load logs, pending logs, and rejections; marks approvals as applied | Delivered pending entries only |
| ETL operator | Load logs, rejections, approvals | Removal approvals after manual review | None |
| Database administrator | All load objects | Grants and versioned DDL | Archive and delete logs after retention, using the procedure below |
| Application reporting role | `analytics.daily_branch_summary` only | None | None |
| `anon` and other application users | None | None | None |

Enable row-level security on the `etl_ops` load tables and revoke PUBLIC access, in the same way as the extraction and transformation log tables. Never expose `etl_ops` through the Supabase Data API. Expose `analytics` only if the application requires it, with SELECT-only grants and row-level security for the reporting role, and never to `anon`.

Archive and delete load records using the [Extraction archive and deletion procedures](../extraction/extraction.md#5-log-retention-and-access). Logs, publication records, rejections, and approvals must not be deleted before one year or while they are needed for troubleshooting, auditing, or recovery. The latest publication record is never deleted while its batch is published. Pending entries must not be removed before delivery.

**Backup:** Routine daily loads rely on transactional rollback. The target can also be rebuilt from source data by rerunning the pipeline. Before a target schema migration, the database administrator confirms a recent Supabase backup exists, copies the current target into restricted storage in `etl_ops`, and records it in the maintenance record.

## 8. Load Acceptance, Rollback, and Completion Rules

| Validation condition | Acceptance criteria | Pipeline action | Decision and responsible role |
| --- | --- | --- | --- |
| All pre-load and post-load critical checks pass, with no holding warning | Batch published exactly and reconciled | `APPROVED`: commit, record the log, report completion | Automatic |
| As above, but the input is `APPROVED_WITH_QUARANTINE` | Load checks pass; Stage 2 quarantine is recorded | `APPROVED`: publish, and show `quarantined_input_records` in the log and the completion report | Automatic; the ETL operator reviews the Stage 2 quarantine evidence |
| Only a storage warning (80–95%, or the limit is not set) | Load checks pass | `APPROVED`: publish and notify the database administrator | Automatic |
| Published history removal without approval | Previously published keys are absent from the batch | `MANUAL REVIEW`: roll back, keep the previous publication, record the held keys | ETL operator and data owner review |
| Any critical check fails, or a SQL error, timeout, or interruption occurs | Batch not acceptable | `REJECTED`: roll back, retry transient errors as in Section 3, otherwise stop | Automatic stop; the ETL operator or database administrator resolves the cause |

Overall precedence: any critical failure gives `REJECTED`; otherwise a holding warning gives `MANUAL REVIEW`; otherwise the batch is `APPROVED`.

Example result for a batch held by the history-removal check:

| Validation summary | Result |
| --- | --- |
| Pre-load checks at receipt | PASS |
| Pre-load checks before writing | WARNING (published history removal) |
| `analytics.daily_branch_summary` load | Rolled back |
| **Overall load** | **MANUAL REVIEW** |

### Manual review

The data owner compares the held keys with the Stage 2 exclusion and quarantine evidence and with recent source corrections.

- If the removal is legitimate, the ETL operator records an approval for those exact keys in `etl_ops.load_review_approvals`. The next run removes only the approved keys and marks the approval as applied. Approvals are valid only for the next `APPROVED` load.
- If the removal is not legitimate, correct the source records and let the next run publish.

Manual review never overrides a failed critical check. The held batch cannot be resumed after its session closes; a fresh run is required.

### Rejection and retries

Only lock and serialization errors are retried: up to 3 publication attempts in total, with waits of 2 and 4 seconds. Schema mismatch, permission failure, stale or duplicate batches, reconciliation failure, statement timeout, and connection loss are not retried automatically.

### Notification

Manual runs print a short summary with what to do next, and `python -m pipeline.scheduling run` prints the same summary for a scheduled run. Scheduled runs raise an exception for `MANUAL REVIEW` and `REJECTED`, so the job appears as failed in `cron.job_run_details`. The ETL operator checks `cron.job_run_details` and `etl_ops.load_logs` every morning before branches open (by 07:00 Asia/Manila). The operator notifies the data owner of manual reviews, and the database administrator of permission, storage, or schema failures.

### Completion and handover

A completed run shows a short terminal summary:

- One line per stage (Extract, Transform, Load) with its decision: OK, ON HOLD, FAILED, or SKIPPED
- The received, new, changed, unchanged, and removed counts
- The quarantined count and the rate of the most affected stream, against its limit
- The date the dashboard data is current through, or the last good run when nothing was published
- What to do next, with the command to run

The command ends with exit code 0 when the batch was published, 2 when it was held for MANUAL REVIEW, and 1 when it was rejected or failed, so a script or task scheduler can tell the outcomes apart.

The full completion report is kept outside the terminal. The run's log file in `logs/` and `etl_ops.load_logs` hold the `load_run_id`, transformation and extraction run IDs, batch window and cutoff, rejected count, `refreshed_at`, and duration.

Downstream users learn of new data through the published rows themselves. Dashboards display "Completed days through the day before `reporting_cutoff`, refreshed `refreshed_at`". When no publication occurs, the previous rows stay visible with an older `refreshed_at`, so users can see that today's refresh has not completed.

## 9. Common Load Problems and Mitigation

| Problem | Possible cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Permanent write rejected | Target written inside the read-only extraction snapshot transaction | `cannot execute INSERT in a read-only transaction`; nothing published | Receive into the load temporary table during the snapshot; publish in a separate read-write transaction in the same session (Section 1) |
| Received batch lost | Connection closes between snapshot commit and publication, or publication runs in another session | Batch cannot be published | Publish in the same session. On loss, keep the previous publication and rerun from extraction |
| Older batch replaces newer data | Manual and scheduled runs overlap after the extraction lock is released | Reports go backwards in time | Dedicated load advisory lock plus the snapshot-freshness check against `etl_ops.load_publications` |
| Duplicate rows or inflated totals after a rerun | Insert-only logic, or the same batch loaded twice | Overstated sales, collections, or expenses | Composite primary key, upsert on the match key, duplicate-batch check |
| Partial publication | Failure partway through without transaction control | Mix of old and new rows | One publication transaction; post-load checks before `COMMIT`; rollback on failure |
| Dashboards blocked or showing an empty table | `TRUNCATE` takes an `ACCESS EXCLUSIVE` lock and is not MVCC-safe | Report timeouts or empty reports | Never truncate. Write with INSERT, UPDATE, and DELETE under `ROW EXCLUSIVE`; readers see the previous publication until commit |
| Published history silently disappears | A source correction, cancellation, or Stage 2 quarantine removes all activity for a date and branch | Historical reports change without explanation | The history-removal check holds publication until removal is approved per key; previous values are kept in `revisions` |
| Foreign-key violation on `branch_id` | Branch deleted after the snapshot | Batch rejected | Reject and rerun. Deactivate branches in the application rather than deleting them; the foreign key also blocks deleting branches that have history |
| Target schema drift | Manual `ALTER` or a migration without a contract update | Insert errors or misaligned columns | Schema-match check; versioned migrations updated together with the Stage 2 `schema_version` |
| Permission denied or excess privileges | A missing Stage 1, Stage 2, or Load grant; or the broad `postgres` account, which can also change source data | The run fails, or a faulty change could reach source data | Privilege pre-check before every publication; the read-only snapshot keeps extraction from writing; move the runs to `ic_etl_runtime` with the Section 1 grants before production |
| Incomplete source history | Row-level security hides some source rows from a pipeline role without `BYPASSRLS`, for example after the move to `ic_etl_runtime` | Summaries understate activity, while extraction counts still match because the same role reads source and staging | `BYPASSRLS` or SELECT policies on the five source tables; compare row counts with the `postgres` account after the move and after policy changes |
| Commit outcome unknown | Connection drops during `COMMIT` | Unclear whether the batch was published | Before any rerun, look up the `load_run_id` in `etl_ops.load_publications`, then finalize the queued log entry; never republish blindly |
| Log delivery fails after publication | Logging transaction fails | Batch published but not fully logged | Pending queue and replay; never repeat the publication because of a log failure. Duplicate and freshness checks read `etl_ops.load_publications`, not the logs |
| Lock waits or timeouts | A long reporting query or maintenance holds a conflicting lock | Load delayed | `lock_timeout` and `statement_timeout`, limited retries, 01:00 off-peak schedule |
| Table bloat | Every row is restamped daily, leaving dead tuples | Gradually slower queries | Rely on autovacuum and monitor dead tuples in `pg_stat_user_tables`; the table is small |
| Storage limit reached | The Supabase plan's database size limit is reached | Writes fail | Storage pre-check, capacity warnings, and log archiving |
| Customer counts overstated | `unique_customer_count` summed across dates or branches | Wrong customer activity totals | Document the column as non-additive; compute multi-day distinct counts from operational data |

Stop publication when a mitigation requires changing business rules, the input contract, or the transaction design. Record the cause and the applied correction, then validate a new batch before publishing.

## Expected Output

One permanent `analytics.daily_branch_summary` table containing exactly the completed-day rows handed over by an approved Stage 2 run. The rows are published atomically with one `load_run_id`, `reporting_cutoff`, and `refreshed_at`, reconciled with the received batch, recorded in `etl_ops.load_publications`, and logged in `etl_ops.load_logs` with lineage to the transformation and extraction runs. Failed or held batches leave the previous publication unchanged. Load adds no new extraction or transformation logic.
