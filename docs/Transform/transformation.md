# I&C Laundry Daily Branch Reporting ETL

## Stage 2: Data Transformation

### Objective

Transform the validated dataset received from the Extraction Stage into cleaned, standardized, and business-ready daily branch data. The output must be traceable and validated for handover to the Load Stage. Loading the permanent reporting table is outside this stage.

## 1. Transformation Input and Processing Specification

### Input source

Transformation receives the approved temporary staging tables defined in the [Extraction Stage handover](../extraction/extraction.md#handover). The input consists of PostgreSQL tables in the same database connection and transaction used by extraction.

| Extraction output | Use in transformation |
| --- | --- |
| `pg_temp.stage_branches` | Resolve branch references and reporting labels using `id` and `branch_name`. |
| `pg_temp.stage_customers` | Resolve customer references for daily customer activity counts. |
| `pg_temp.stage_orders` | Prepare order dates, statuses, weights, and order values for daily branch measures. |
| `pg_temp.stage_payments` | Prepare collection dates and amounts for daily branch payment measures. |
| `pg_temp.stage_expenses` | Prepare expense dates, amounts, and deletion status for daily branch expense measures. |

The handover must include the extraction `pipeline_run_id`, run-start timestamp, staged counts, validation results, and either `APPROVED` or `APPROVED_WITH_WARNINGS` acceptance status. Transformation must acknowledge the extraction run ID and acceptance decision before consuming the staged records. `MANUAL REVIEW` and `REJECTED` batches must not proceed.

The received batch contains all available history. Transformation must use the extraction run-start timestamp to establish one reporting cutoff: the start of that run's calendar date in **Asia/Manila**. Only business dates earlier than this cutoff contribute to completed-day reporting. Current-day and future-dated records must be counted separately from reporting contributions.

Transformation must read the supplied staging tables, not re-extract records from the source system.

### Batch processing method

SQL inside PostgreSQL performs cleaning, standardization, business calculations, and validation. Records remain in the database.

- **Rules:** `etl_ops.consume_transformation_strict(jsonb)` applies the rules in Sections 3 to 5 and the checks in Section 7.
- **Tolerance:** `etl_ops.consume_transformation(jsonb)` wraps it and applies the quarantine tolerance in Section 10.
- **Profiling:** a separate read-only step, `python -m pipeline.profiling` (Section 2).
- **Manual runs:** `pipeline/transformation.py` calls the same function on the extraction connection. `python -m pipeline.transformation` runs Stages 1 and 2 only, and `python -m pipeline.load` runs all three stages.
- **Scheduled runs:** the extraction procedure calls `etl_ops.consume_transformation_with_load(jsonb)`, which runs Stage 2 and passes an approved result to Load.

Transformation never modifies the raw extraction stages. It reads the staged rows directly and applies cleaning and standardization as expressions in its queries, for example `lower(btrim(status))`. It writes only to derived temporary tables. `etl_ops.prepare_transformation()` creates those empty tables in the setup transaction, before the read-only extraction snapshot begins:

- `transformation_issues`
- `transformation_accounting`
- `transformation_rules`
- `transformed_daily_branch_summary`

They are populated within the shared snapshot and never created inside the read-only transaction.

The batch runs daily immediately after approved extraction in the **01:00 Asia/Manila** pipeline execution. It uses the active extraction handover rather than a separately scheduled connection, because temporary staging is session-local.

Each batch must process the full staged history and rebuild the eligible completed-day measures. This allows subsequent batches to incorporate historical corrections and late collections. Orders use their creation dates, payments use their collection dates, and expenses use their recorded expense dates.

If transformation fails, stop handover to Load and discard incomplete transformed output. After the transaction ends, restart through the Extraction Stage to obtain a new approved snapshot and extraction run ID. Transformation must not attempt to reuse temporary tables from a closed connection.

Reruns must be idempotent: the same staged input, reporting cutoff, and transformation rules must produce the same business rows without appending duplicates. At the start of each batch, the temporary issue, accounting, rule, and transformed-output tables are cleared before they are repopulated. A new extraction snapshot may legitimately change historical results when source records have been corrected.

### Transformation scope

| Input | Fields processed | Transformation scope |
| --- | --- | --- |
| `stage_branches` | `id`, `branch_name` | Prepare consistent branch labels and provide branch lookups. |
| `stage_customers` | `id` | Provide customer lookups without adding personal information. |
| `stage_orders` | `id`, `order_number`, `branch_id`, `customer_id`, `created_at`, `status`, `weight_kg`, `total_price` | Retain identifiers for traceability; standardize reporting dates and statuses; apply documented eligibility rules; calculate order counts, released-order counts, distinct customer counts, total weight, and order value. |
| `stage_payments` | `id`, `order_id`, `branch_id`, `paid_at`, `amount` | Retain payment and order references; standardize collection dates; calculate payment counts and cash received. |
| `stage_expenses` | `id`, `branch_id`, `expense_date`, `amount`, `deleted_at` | Retain expense references and dates; exclude soft-deleted expenses with recorded counts; calculate expense totals. |

Orders, payments, and expenses must be aggregated independently by business date and branch before their results are combined. The resulting dataset must contain one row per reporting date and branch for handover to Load. Sections 3 to 5 define the detailed cleaning, standardization, and business rules.

### Dependencies and assumptions

- Process branch and customer reference lookups before the related transaction datasets. Resolve payment order references against staged orders before aggregating payments.
- Required staging tables, selected columns, and extraction metadata must match the Stage 1 handover contract. Missing staging or unavailable lookups stop the batch; transformation must not fetch replacement source data.
- A fact row without a resolvable branch or reporting date is quarantined under C-06 and counts toward the Section 10 rate. Transformation never assigns a default branch or date.
- Convert order and payment timestamps to Asia/Manila business dates. Preserve `expense_date` as its existing calendar date.
- Monetary values represent PHP and weights represent kilograms. Use exact PostgreSQL `NUMERIC` arithmetic for calculations.
- Full-history processing includes late payments and historical corrections in subsequent runs. Its workload grows with the staged history.
- Temporary staging and transformed output must be consumed before the active transaction and connection end. The extraction transaction permits writes to temporary tables but is read-only for permanent tables. Load therefore copies the approved output during the snapshot and publishes it afterwards in a separate read-write transaction in the same session ([Load Stage, Section 1](../Load/Load.md#transaction-design)).

## 2. Data Profiling and Issue Identification

### Profiling method

Profile the approved staging tables before applying cleaning, standardization, or business rules. SQL must return counts, value distributions, and a few relevant sample values within the active extraction connection. Profiling must not modify the staged records or re-extract source data.

Associate every profile with its extraction run ID and reporting cutoff. For each issue, record the affected table and column, issue type, observed sample values, affected count and percentage, and planned treatment. Calculate the percentage using the number of received rows in that table. For an empty table, record a count of zero and percentage as not applicable. Count each row once per issue; counts from different issues may overlap and must not be added as a total of rejected records.

### Available batch evidence

Extraction run `d5d87bcf-e5cb-4da1-828e-1b3379e1674b` recorded the following counts and validation results. These findings apply to that run only.

| Staging table | Received rows | Findings from extraction validation |
| --- | --- | --- |
| `stage_branches` | 3 | No missing or duplicate IDs; source and staging counts matched. |
| `stage_customers` | 3 | No missing or duplicate IDs; source and staging counts matched. |
| `stage_orders` | 4 | No missing or duplicate IDs or order numbers; no missing or unresolved branch/customer references; no missing or non-finite creation dates. |
| `stage_payments` | 4 | No missing or duplicate IDs; no missing or unresolved branch/order references; no missing or non-finite payment dates. |
| `stage_expenses` | 3 | No missing or duplicate IDs; no missing or unresolved branch references; no missing or non-finite expense dates. |

The affected count for each listed extraction check was zero. Label quality, status consistency, numeric validity, and reporting eligibility require separate transformation profiling. Temporary staging is removed when the extraction connection closes; subsequent profiling must use a new approved handover and record its extraction run ID.

### Transformation profiling register

Profiling run `ab6ed3e7-e577-4325-a705-c7dedd349a71` executed on 2026-10-23 at 19:45:53 Asia/Manila through the approved extraction handover using `python -m pipeline.profiling`. It measured 3 branches, 3 customers, 4 orders, 4 payments, and 3 expenses. The reporting cutoff is 2026-10-23 in Asia/Manila.

| Measured check | Result | Treatment reference |
| --- | --- | --- |
| Missing/blank, surrounding whitespace, or control characters in branch labels | 0 of 3 (0%) for each check | C-01, C-02; preventive controls |
| Order status distribution | cancelled, ready, received, released: 1 of 4 (25%) each | S-03, B-02, B-04 |
| Missing/blank, surrounding whitespace, or inconsistent case in order status | 0 of 4 (0%) for each check | C-01, C-02, S-03 |
| Negative order weight or value, missing weight/value, non-finite weight/value | 0 of 4 (0%) for each check | C-03, C-04, C-08 |
| Negative, missing, or non-finite payment amount | 0 of 4 (0%) for each check | C-03, C-08 |
| Negative, missing, or non-finite expense amount | 0 of 3 (0%) for each check | C-03, C-08 |
| Numeric ranges | Orders: 8-12 kg and PHP 200-300; payments: PHP 100-300; expenses: PHP 150-450 | Review finite ranges when larger source volumes are available |
| Business dates and cutoff | Orders and payments: 2026-10-12 to 2026-10-13, 4 before cutoff, 0 on, 0 after; expenses: 2026-10-12 to 2026-10-13, 3 before cutoff, 0 on, 0 after | S-01, S-02, B-01 |
| Cancelled orders | 1 of 4 (25%); 8 kg and PHP 200 order value | B-02; business exclusion |
| Soft-deleted expenses | 1 of 3 (33.33%) | B-03; business exclusion |
| Payment/order branch mismatch | 0 of 4 payments (0%) | C-07 |
| Multiple payments per order | 1 of 3 paid orders (33.33%) has 2 payments; the other 2 have 1 each | B-05; preserve all distinct payment records |

The following register records the measured findings for the approved profiling run. The values are specific to that run and must be measured again when the source data changes materially.

| Table and column | Issue or characteristic to inspect | Illustrative sample | Affected records | Planned treatment |
| --- | --- | --- | --- | --- |
| `stage_branches.branch_name` | NULL or blank labels, leading/trailing whitespace, and control characters | No affected labels | 0 of 3 (0%) for each check | Section 3: retain preventive label-cleaning controls. |
| `stage_orders.status` | NULL or blank statuses, inconsistent case/spacing, and distinct status values | cancelled, ready, received, released | 0 of 4 (0%) formatting issues; each status occurs once | Section 4: retain the approved lowercase status mapping. |
| `stage_orders.weight_kg`, `stage_orders.total_price` | NULL, negative, non-finite, or unusually large values | Weight: 8-12 kg; value: PHP 200-300 | 0 of 4 (0%) invalid values | Section 3: retain invalid-value handling; Section 4: retain NUMERIC precision. |
| `stage_payments.amount`, `stage_expenses.amount` | NULL, negative, non-finite, or unusually large amounts | Payments: PHP 100-300; expenses: PHP 150-450 | 0 of 4 payments and 0 of 3 expenses invalid | Section 3: retain amount controls and re-profile as volume grows. |
| `stage_orders.created_at`, `stage_payments.paid_at`, `stage_expenses.expense_date` | Earliest/latest business dates and counts before, on, or after the reporting cutoff | 2026-10-12 to 2026-10-13 | Orders: 4/0/0; payments: 4/0/0; expenses: 3/0/0 before/on/after cutoff | Section 4: apply Manila dates; Section 5: apply the completed-day window. |
| `stage_orders.status` | Number of cancelled orders and their associated values | One cancelled order: 8 kg, PHP 200 | 1 of 4 (25%) | Section 5: exclude it from order measures under B-02. |
| `stage_expenses.deleted_at` | Number of soft-deleted expenses | One non-NULL deletion timestamp | 1 of 3 (33.33%) | Section 5: exclude it from reporting under B-03. |
| `stage_payments.branch_id`, linked `stage_orders.branch_id` | Different valid branches on a payment and its order | No mismatched pair | 0 of 4 payments (0%) | Section 5: retain C-07 review control. |
| `stage_payments.order_id` | Distribution of payment rows per order | One order has two payments | 1 of 3 paid orders (33.33%) has two payments; two have one each | Section 5: aggregate distinct payment rows independently. |

Business exclusions, date-window deferrals, and multiple payments per order are distinct from data quality errors. Profiling must preserve all staged records. Each observed issue must reference its cleaning, standardization, or business rule in Sections 3, 4, or 5. A completed profile must contain measured findings and planned treatments for an identified batch.

### Issues found by later runs

Every Stage 2 run records its findings as quarantine evidence (Section 9), so issues that appear after profiling are measured too. Between 2026-11-04 and 2026-11-12, 26 runs quarantined the same record:

| Table and column | Issue type | Sample | Affected records | Treatment |
| --- | --- | --- | --- | --- |
| `stage_orders.weight_kg` | Invalid value (negative weight) | `-2` on order `INVALID-NEGATIVE-WEIGHT` | 1 of 35 orders (2.9%) in the latest of those runs | C-04: quarantined, within the 5% tolerance. Corrected at the source on 2026-11-12 with `python -m pipeline.quarantine --fix` (`-2` to `2`). |

## 3. Data Cleaning Rules

Cleaning never changes raw staging. The rules evaluate the staged rows directly, and a correction such as trimming applies only to the derived values. The measured profiling findings in Section 2 identify the targets below. Rule counts must distinguish changed, quarantined, and retained rows. A row-level issue in `orders`, `payments`, or `expenses` is evaluated by the tolerance policy in Section 10: the affected row may be quarantined while unrelated valid rows continue. Schema, source-access, dimension, reconciliation, and output-contract failures remain batch-stopping failures.

| Rule ID | Target | Condition | Action | Justification |
| --- | --- | --- | --- | --- |
| C-01 | Branch labels and order statuses | Leading or trailing whitespace | Trim working values; count changed rows | Prevent formatting differences from affecting labels or status mapping |
| C-02 | Required labels and statuses | NULL, blank after trimming, or embedded control characters | Quarantine the affected fact row and log it; a bad branch or customer dimension stops the batch | No reliable replacement can be inferred |
| C-03 | Weight and monetary fields | NULL or non-finite numeric value | Quarantine the affected fact row and log it | These values cannot support valid reporting totals |
| C-04 | Order weight | Negative value | Quarantine the affected order and log it | Negative weight cannot represent laundry demand |
| C-05 | IDs and order numbers | Duplicate identifier in the staged data | Quarantine every duplicate fact row; a duplicate branch or customer dimension stops the batch | No extracted update timestamp establishes which version is authoritative |
| C-06 | Required references and dates | Missing value or unmatched staged parent | Quarantine the affected fact row; a failed branch or customer dimension stops the batch | Prevent incorrect branch, customer, order, or date attribution |
| C-07 | Payment branch versus order branch | Different valid branch IDs | Quarantine the payment and log it for review | Cross-branch collection handling requires an approved business rule |
| C-08 | Order value and expense amount | Negative value | Quarantine the affected row and log it for review | Refund and adjustment semantics must be defined before negative amounts contribute to reporting |
| C-09 | Payment amount | Zero or negative value | Quarantine the payment and log it | The source schema requires payment amount greater than zero |

Duplicate detection uses identifiers, not repeated amounts or payment order references. Multiple payments for one order are valid distinct records when payment IDs differ. When a duplicate identifier is found, no copy is kept, because the source provides no update timestamp for selecting an authoritative version. Every copy of a duplicated order, payment, or expense is quarantined. A duplicated branch or customer stops the batch.

Unusually large finite values must be reviewed using measured profiles; no unverified threshold may silently remove them. Business exclusions such as deleted expenses belong to Section 5.

Each rejected row must be counted once, with all applicable rule IDs retained. Modified rows remain part of retained input. Missing values are never replaced with zero merely to make calculations succeed. Section 2 links each measured finding to the rule that treats it.

## 4. Standardization and Data Type Rules

Standardization preserves business meaning and applies to the cleaned values derived from the staged rows.

| Rule ID | Target | Standard and conversion | On failure |
| --- | --- | --- | --- |
| S-01 | Order and payment timestamps | Derive DATE using `(timestamp_value AT TIME ZONE 'Asia/Manila')::date`; retain original TIMESTAMPTZ for traceability | Quarantine a row with a missing or non-finite timestamp; recorded under C-06 |
| S-02 | Expense date | Preserve DATE without timezone conversion; display as YYYY-MM-DD | Quarantine a row with a missing or non-finite date; recorded under C-06 |
| S-03 | Order status | Lowercase trimmed text; validate against a versioned approved status mapping | Quarantine an order with an unknown code and log it for review; do not guess its meaning |
| S-04 | Branch label | Preserve spelling and case after C-01 trimming | Reject unusable labels under C-02 |
| S-05 | Weight and amounts | Use exact NUMERIC for intermediate calculations; round each final group total once to two decimals for NUMERIC(14,2) | Reject non-finite values or totals outside the target range |
| S-06 | Identifiers and counts | Retain UUID identifiers; use BIGINT counts | An incompatible input type stops extraction (Stage 1 schema check); an overflowing total is recorded under S-05 |
| S-07 | Run timestamps | Preserve TIMESTAMPTZ; display ISO 8601 with timezone offset | Unparseable run metadata fails the run with `TRANSFORMATION_FAILED` |

Currency is PHP and weight is kilograms. No currency or unit conversion is performed. Numeric source values are already typed; do not convert through floating-point arithmetic or formatted currency strings.

The status mapping, unchanged since rule version `1.0.0`, recognizes the source schema's codes `received`, `on_process`, `ready`, `released`, and `cancelled`. The first four contribute to eligible order measures; `released` also contributes to released-order counts. Cancelled orders are excluded from order measures. Case and whitespace normalization does not authorize mapping unfamiliar codes to existing statuses. An order with an unknown status is quarantined and counts toward the Section 10 rate, so a widespread new status holds or rejects the batch. The rule version and the changed-row counts (C-01, S-03) are recorded in the metadata.

Rounding must occur after aggregation, not once per source row. Validate the rounded result before casting to NUMERIC(14,2). Conversion failures must be logged with the rule ID and affected record or group; they must not become silent NULLs or zeroes.

## 5. Business Transformation Rules

Transform eligible working records into one daily branch summary. Reuse the captured reporting cutoff for the entire batch.

| Rule ID | Input columns | Logic | Output column | Business reason |
| --- | --- | --- | --- | --- |
| B-01 | `orders.created_at`, `payments.paid_at`, `expenses.expense_date` | Use the S-01/S-02 business date; keep dates earlier than the cutoff and count later rows as deferred | (filter) | Reports cover completed days only, because the current day is still changing |
| B-02 | `orders.status` | Exclude orders mapped to `cancelled`; record the count and reason | (filter) | Cancelled orders are not laundry demand or order value |
| B-03 | `expenses.deleted_at` | Exclude expenses with a non-NULL `deleted_at`; record the count | (filter) | Deleted expenses were withdrawn in the application |
| B-04 | Eligible orders: `branch_id`, `customer_id`, `status`, `weight_kg`, `total_price` | Group by order business date and `branch_id`; count orders, count `released` orders, count distinct `customer_id`, sum weight and value | `order_count`, `released_order_count`, `unique_customer_count`, `total_weight_kg`, `order_value_php` | Daily demand, completions, customer activity, and order value per branch |
| B-05 | Eligible payments: `branch_id`, `amount` | Group by collection business date and payment `branch_id`; count and sum | `payment_count`, `cash_received_php` | Cash is reported on the day it was collected, separately from order value |
| B-06 | Eligible expenses: `branch_id`, `amount` | Group by `expense_date` and `branch_id`; sum | `expense_total_php` | Daily branch expenses on their recorded date |
| B-07 | B-04, B-05, and B-06 results | Union the distinct date and branch keys, then left join each aggregate on both keys; 0 for an absent stream | `report_date`, `branch_id`, and all measures | One row per date and branch without multiplying detail rows |
| B-08 | `orders.branch_id`, `orders.customer_id`, `payments.order_id`, `payments.branch_id`, `expenses.branch_id` | Each reference must match exactly one staged parent (checked by C-06) | (lookup) | Stable branch and customer attribution without row multiplication |

For B-04, count eligible orders, count those mapped to released, count distinct customer_id, and sum weight_kg and total_price. Released orders are grouped by creation date; no release-event timestamp is available. A status change can therefore revise historical results.

Payments remain independent ledger entries. A payment on a cancelled order is not discarded solely because that order is excluded from order metrics. Negative amounts and cross-branch collections are quarantined under C-07 and C-08 until the source record is corrected or a business rule is approved. The cancellation and release mappings are approved in the rule version; unknown statuses are never treated as eligible.

Never join raw orders, payments, and expenses together for aggregation. Each stream must first produce unique date/branch keys. A day with payments or expenses but no eligible orders must remain in the output. A date/branch with no eligible activity in any stream has no output row.

No surrogate keys, dimension tables, or slowly changing dimensions are introduced. Branch IDs remain the grouping keys; labels come from staged branch references.

Assign each input row one accounting category in this order: rejected, business-excluded, deferred, or contributing. Record additional matched reasons separately without double-counting exclusions. Preserve counts and source identifiers for traceability.

## 6. Transformed Data Contract and Naming Convention

The Load Stage receives `pg_temp.transformed_daily_branch_summary` in the active connection. Every column is NOT NULL, and the composite primary key `(report_date, branch_id)` is enforced by the temporary table itself. Branch IDs resolve to staged branches. The table has no surrogate or foreign keys; Load adds the foreign key to `public.branches`.

| Source column | Source type | Output column | Output type | Transformation applied |
| --- | --- | --- | --- | --- |
| `orders.created_at`, `payments.paid_at`, `expenses.expense_date` | TIMESTAMPTZ, TIMESTAMPTZ, DATE | `report_date` | DATE | Asia/Manila business date (S-01, S-02); union of the stream keys (B-07) |
| `orders.branch_id`, `payments.branch_id`, `expenses.branch_id` | UUID | `branch_id` | UUID | Preserved; key of each independent aggregate (B-04 to B-07) |
| `orders.id` | UUID | `order_count` | BIGINT | Count of eligible orders (B-04); 0 when none |
| `orders.status` | TEXT | `released_order_count` | BIGINT | Count of eligible orders mapped to `released` (S-03, B-04); 0 when none |
| `orders.customer_id` | UUID | `unique_customer_count` | BIGINT | Distinct customers per date and branch (B-04); 0 when none |
| `orders.weight_kg` | NUMERIC | `total_weight_kg` | NUMERIC(14,2) | Sum, rounded once per group (B-04, S-05); 0 when none |
| `orders.total_price` | NUMERIC | `order_value_php` | NUMERIC(14,2) | Sum, rounded once per group (B-04, S-05); 0 when none |
| `payments.id` | UUID | `payment_count` | BIGINT | Count of payments (B-05); 0 when none |
| `payments.amount` | NUMERIC | `cash_received_php` | NUMERIC(14,2) | Sum, rounded once per group (B-05, S-05); 0 when none |
| `expenses.amount` | NUMERIC | `expense_total_php` | NUMERIC(14,2) | Sum of non-deleted expenses, rounded once per group (B-03, B-06, S-05); 0 when none |

Use lowercase snake_case for tables and columns. The `transformed_` prefix identifies temporary Stage 2 output, while `analytics.daily_branch_summary` is the permanent destination owned by Load. Retain branch_id as the UUID key; do not introduce a branch surrogate key. Validate uniqueness and required values before handover.

The permanent destination remains `analytics.daily_branch_summary`. The Load Stage supplies `refreshed_at` when publication succeeds; transformation completion time is recorded separately in metadata.

Input schema must match the Stage 1 contract. Missing required fields or incompatible types stop the batch. Additional unused fields do not alter transformation output. Version any intentional input mapping or output schema change and verify Load compatibility before deployment.

## 7. Transformation Validation and Data Quality Checks

Validation must assess transformed data and its contributing staged records. It must not repeat source connectivity or extraction execution checks.

Each check is recorded under a rule or check ID. Row-level findings in `orders`, `payments`, or `expenses` quarantine only the affected row and count toward the Section 10 rate. Critical failures stop the batch.

| Check name | Target | Purpose | Validation criteria | Severity |
| --- | --- | --- | --- | --- |
| Row accounting (`V-ACCOUNTING`) | Every staged table | Detect lost or double-counted rows | For each table, received = rejected + excluded + deferred + contributing | Critical |
| Identifier uniqueness (C-05) | IDs and `orders.order_number` | Prevent duplicate rows in totals | No duplicate or missing identifier | Critical for branches and customers; row-level for transaction streams |
| Reference resolution and join cardinality (C-06, B-08) | Branch, customer, and order references | Prevent orphan rows and row multiplication in joins | Every reference matches exactly one staged parent | Row-level |
| Required dates (C-06) | `created_at`, `paid_at`, `expense_date` | Assign every row to one business date | Date present and finite | Row-level |
| Numeric validity (C-03, C-04, C-09) | Weights and amounts | Keep invalid values out of totals | Finite values; weight 0 or more; payment amount more than 0 | Row-level |
| Policy review (S-03, C-07, C-08) | Status codes, cross-branch payments, negative order values and expenses | Hold values that need a business decision | Approved status code; payment branch equals order branch; amount 0 or more | Row-level (warning) |
| Output key coverage (`V-KEYS`) | Output `(report_date, branch_id)` keys | Detect missing or extra date and branch rows | Output keys equal the union of the eligible stream keys; the output primary key rejects duplicates | Critical |
| Reporting window, count relationships, and group reconciliation (`V-TOTALS`) | Every output row | Detect wrong dates, impossible counts, and lost values | `report_date` is before the cutoff; released and unique-customer counts do not exceed `order_count`; each group's counts and rounded sums equal its eligible staged rows, recomputed independently of the aggregation | Critical |
| Required fields and types (S-05) | Output contract | Keep the Section 6 contract | NOT NULL and type constraints of the output table hold; no total overflows NUMERIC(14,2) | Critical |
| Empty reporting output (`V-EMPTY`) | Whole batch | Avoid replacing published history with nothing | At least one output row | Non-critical (warning); holds the batch for MANUAL REVIEW |

The output checks `V-ACCOUNTING`, `V-KEYS`, and `V-TOTALS` run when no row is quarantined. When rows are quarantined, Stage 2 rebuilds the output from the remaining rows with the same B-rules (Section 10) and recomputes the row accounting. It does not repeat those three checks on the rebuilt output; Load's exact-content and measure checks then confirm that the published rows match it.

Modified retained rows are not an extra term in the row-accounting equation. A row can match several rules but belongs to only one final accounting category.

Aggregate row count is not expected to equal input row count. Reconcile contributing rows and measures per stream and per date/branch. Distinct customer counts must be compared within each group rather than against one global distinct count.

Compare rounded group sums with the same per-group rounding method. The sum of rounded groups may differ from a rounded grand total; record the difference without hiding a failed group check.

Any schema, dimension, reconciliation, or output-contract failure rejects the batch. Row-level fact issues use the tolerance policy in Section 10. A failed check is recorded as an issue with its rule ID, severity, and reason in `validation_results`; passing checks add no entry. Affected counts per rule are in `rules_applied`, and row counts in `accounting` (Section 8).

## 8. Transformation Metadata and Log Specification

Store one row per transformation run in restricted `etl_ops.transformation_logs`, with the columns `run_id`, `parent_run_id`, `ended_at`, and `metadata`. The `metadata` JSONB document holds the fields below. Per-table counts are kept in its `accounting` list, one entry per staged table, so aggregate output counts are not confused with contributing source counts. Transformation and extraction run identities stay distinct and linked.

| Field | Purpose | Example or format |
| --- | --- | --- |
| `run_id` | Unique ID of the transformation run | UUID |
| `parent_run_id` | Linked extraction `pipeline_run_id`; Stage 1 metadata is referenced, not copied | UUID |
| `batch_id` | Logical batch: extraction run, cutoff, and batch version | `<extraction run ID>:2026-11-12:1.0.0` |
| `cutoff` | Exclusive end of the batch window; the window is all staged history before this Asia/Manila date | `2026-11-12` |
| `started_at`, `ended_at` | Transformation start and end | `2026-11-12T01:19:52.277819+00:00` |
| `duration_ms` | Transformation runtime | `14` |
| `status` | Validation outcome | `PASS`, `PARTIAL`, `WARNING`, `FAIL` |
| `acceptance_status` | Handover decision (Section 10) | `APPROVED`, `APPROVED_WITH_QUARANTINE`, `MANUAL REVIEW`, `REJECTED` |
| `handover` | Whether Load received the output | `ACKNOWLEDGED`, `NOT_CONFIGURED`, `NOT_ATTEMPTED`, `FAILED` |
| `log_delivery_status` | Log persistence, separate from the data decision | `PENDING`, `SAVED`, `FAILED` |
| `accounting` | Per staged table: `table_name`, `records_read`, `records_rejected` (quarantined), `records_excluded`, `records_deferred`, `records_contributing`, and `records_modified` | `{"table_name": "orders", "records_read": 35, "records_rejected": 1, ...}` |
| `records_output` | Rows in the transformed output | `33` |
| `quarantined_records` | Distinct quarantined rows across the transaction streams | `1` |
| `quarantine_rates` | Quarantined rows, received rows, and rate for `orders`, `payments`, and `expenses` | `{"orders": {"quarantined": 1, "received": 35, "rate_percent": 2.9}, ...}` |
| `quarantine_table`, `quarantine_rate_percent` | The stream with the highest rate, and that rate, which drives acceptance | `orders`, `2.9` |
| `tolerance` | The limits applied (Section 10) | `{"auto_publish_max_percent": 5, "manual_review_max_percent": 10}` |
| `rules_applied` | Rows affected per rule and table; counts may overlap | `[{"rule_id": "C-04", "table_name": "orders", "affected_rows": 1}, ...]` |
| `issues` | Each finding: table, record UUID, rule ID, severity, and reason | `{"table_name": "orders", "rule_id": "C-04", "severity": "FAIL", "reason": "Negative numeric value", ...}` |
| `validation_results` | Overall validation status and the issue list | `{"status": "PARTIAL", "issues": [...]}` |
| `error_code`, `error_message` | Failure category and sanitized description when the batch is held or rejected | `QUARANTINE_REVIEW_REQUIRED`, `QUARANTINE_LIMIT_EXCEEDED`, `TRANSFORMATION_FAILED`, or absent |
| `code_version`, `rule_version`, `schema_version` | Implementation, rule-set, and output-contract versions | `1.0.0`, `1.2.0`, `1.0.0` |

Every run records `rule_version` `1.2.0`. The quarantine counts, rates, and `tolerance` are recorded when the Section 10 policy evaluates the batch, including clean runs (0 quarantined, 0% per stream). A batch with no output left after quarantining records `quarantined_records` only. A batch stopped by a schema, dimension, reconciliation, or empty-output check records `tolerance` with its issues and status, but no quarantine counts.

- **Monitoring:** Track the daily decision, duration, output rows, and quarantine rate per stream.
- **Troubleshooting:** The `issues` list names the rule, table, record, and reason; `error_code` explains a hold or rejection.
- **Auditing:** `rules_applied` and `rule_version` show which rules changed, excluded, or quarantined how many rows in each run.
- **Lineage:** `parent_run_id` links to the extraction log. Load stores this `run_id` as `transformation_run_id`, and the quarantine evidence and fix list reference it.
- **Recovery:** A held or rejected run is never resumed. After the source records are corrected, for example with `python -m pipeline.quarantine --fix`, a new run starts from extraction, and its `accounting` can be compared with the failed run.

Do not duplicate all extraction metadata or store credentials in diagnostics.

Failure evidence must survive rollback. The orchestration layer must capture diagnostics before ending the processing transaction and persist them through a separate authorized logging transaction afterward. Permanent writes must not be attempted inside the existing read-only extraction transaction.

Undelivered metadata must remain in a restricted pending store until delivery commits. Replay must preserve original run IDs and timestamps and avoid duplicate entries. If a connection fails before diagnostic evidence is durably captured, record incomplete logging and inspect available scheduler/service evidence; do not claim guaranteed recovery.

Keep data acceptance separate from log-delivery status. Validation determines whether temporary output may be handed over while the transaction is active. Final-log persistence is confirmed after that transaction ends. Record log delivery as PENDING, SAVED, or FAILED, and do not report the run as fully logged until delivery is SAVED. A logging failure must not trigger an automatic repeat of a downstream operation that may already have completed.

## 9. Log Retention, Rejected Data, and Access

Transformation logs and rejection evidence must be retained for at least one year to investigate historical report changes and failed batches. Longer organizational audit requirements take precedence.

| Record | Storage | Contents and ownership |
| --- | --- | --- |
| Transformation logs | etl_ops.transformation_logs | Counts, rule actions, validation outcomes, decisions, and lineage; owned by authorized ETL operations role |
| Quarantine evidence | etl_ops.transformation_quarantine | One row per finding: the transformation `run_id`, which links to the batch ID in the log, plus the staged table, record UUID, rule ID, severity, and reason. Offending values are not copied; the fix list reads the current values from the source. Owned by authorized ETL operations role |
| Quarantine fix list | etl_ops.quarantine_review (view) | Records quarantined by the latest run, with their current source values, the rules they broke, how to fix them, and how many runs have quarantined them |
| Quarantine fix log | etl_ops.quarantine_fixes | One row per source field changed through the fix command: who and when, table, record UUID, rules, field, old and new values, and the run that quarantined the record; owned by authorized ETL operations role |
| Pending metadata | Restricted orchestration pending store | Undelivered diagnostic metadata; retained until durable delivery is confirmed |

Quarantine must contain only the fields needed to diagnose a failed transformation. Do not copy complete customer records or unnecessary sensitive data. Business-excluded and deferred records are not quarantined as invalid records; record their counts and reasons separately.

Quarantine is not a substitute for extraction staging. After correction, rerun extraction and transformation against a new approved snapshot.

Fix a quarantined record in its source table, for example in the Supabase Table Editor, not in the quarantine evidence. The pipeline reads the full source history on every run, so a corrected copy would be quarantined again and the application would keep the wrong value. `python -m pipeline.quarantine` lists the records quarantined by the latest run with their current values, the rule they broke, and how to fix them; `python -m pipeline.quarantine --export` also saves the list as CSV in `quarantine/`.

`python -m pipeline.quarantine --fix` corrects the records from the terminal, one record at a time:

- It changes only the source fields that the record's rules flagged, and only those that still break a rule.
- Each new value is checked against the same rule before it is accepted. Weight and order value must be 0 or more, a payment amount must be more than 0, a status must be an approved status, and a payment's branch must be its order's branch.
- Nothing is saved until the operator confirms. The record's changes and their fix-log rows are written in one transaction.
- A change is refused when the field no longer holds the value that was shown, so a newer edit made in the application is not overwritten.
- Rules that need more than a field correction, such as removing a duplicate (C-05), are left to the Supabase Table Editor.

The next run checks each record again, and a corrected record leaves the list. Retain rejection evidence even when the failed processing transaction rolls back through the logging arrangement in Section 8.

The runtime may write through authorized logging paths. Approved operators and administrators may read. Only authorized administrators may archive or delete. Enforce restricted database privileges and appropriate row-level policies before deployment.

Archive older records within restricted PostgreSQL storage when needed. Verify counts, run IDs, reasons, and timestamps before treating an archive as complete. Archive copies remain subject to the original retention and access requirements.

Before deletion, an administrator must confirm that the retention period has elapsed and the records are not needed for troubleshooting, audit, or recovery. Record the approval, run IDs, expected and actual counts, and outcome in a separate maintenance audit record. Pending entries must not be removed before successful delivery. Audit records follow their own applicable retention period.

## 10. Transformation Acceptance and Handover Rules

Stage 2 decides on its own whether a batch with quarantined rows is still usable. For each transaction stream (`orders`, `payments`, and `expenses`), the quarantine rate is the number of distinct quarantined rows divided by the rows received in that stream. The stream with the highest rate decides:

| Highest stream quarantine rate | Decision |
| --- | --- |
| 5% or less | APPROVED_WITH_QUARANTINE: the valid rows are offered to Load |
| More than 5%, up to 10% | MANUAL REVIEW: the output is held and not offered |
| More than 10% | REJECTED: the output is not usable |

A quarantined row never contributes to analytics, so daily totals are understated by about the quarantine rate, and defects usually concentrate in one branch or day. Up to 5%, the published totals stay close and the quarantined count is reported. Between 5% and 10%, a person must decide whether the totals are still acceptable. Above 10%, branch comparisons and trends become misleading. Each stream is measured on its own so that a badly affected smaller stream, such as expenses, is not hidden by a large clean one. These limits are part of rule version `1.2.0`, which every Stage 2 run records with the tolerance it applied. A clean run records 0 quarantined rows and a 0% rate for each stream.

The output and metadata must explicitly record the quarantined count, the rate of each stream, and the rule evidence, so the system does not silently omit a row. Business exclusions and date-window deferrals are not quarantines and do not count toward the rate.

Schema, extraction-contract, branch/customer dimension, aggregate reconciliation, and output-contract failures have **0% tolerance** because they can make unrelated analytics unreliable. For an empty stream, record its rate as not applicable; an empty output follows the empty-output review rule.

| Validation condition | Acceptance | Pipeline action | Decision and responsibility |
| --- | --- | --- | --- |
| All critical checks pass; zero quarantined rows; no unresolved warnings; required validation and handover metadata captured | APPROVED | Offer validated output to Load; finalize persistent logging after the transaction ends | Automatic |
| Only independently attributable fact rows are quarantined, at least one valid output group remains, and the highest stream quarantine rate is 5% or less | APPROVED_WITH_QUARANTINE | Offer the partial output; preserve quarantine evidence and counts | Automatic; operator fixes the listed records separately |
| No critical failure, but an empty-output warning remains; or the highest stream quarantine rate is more than 5% and up to 10% | MANUAL REVIEW | Hold handover; discard the temporary output; preserve evidence and the fix list | ETL operator and data owner review; fix the records and run again |
| Any schema, dimension, reconciliation, output-contract, or operational failure; no valid output remains after quarantining; or the highest stream quarantine rate is more than 10% | REJECTED | Stop handover and discard incomplete output | Automatic stop; operator resolves the cause |

Example validation summary for a batch with 3 of 35 orders quarantined:

| Validation summary | Result |
| --- | --- |
| `branches` and `customers` references | PASS |
| `orders` transformation | WARNING (3 of 35 quarantined, 8.6%) |
| `payments` transformation | PASS (0 of 34 quarantined) |
| `expenses` transformation | PASS (0 of 33 quarantined) |
| **Overall transformation** | **MANUAL REVIEW** (`QUARANTINE_REVIEW_REQUIRED`) |

`APPROVED_WITH_QUARANTINE` does not repair source data. Operators must correct quarantined records and review later runs. Manual review does not override failed reconciliation or convert an unsupported value into valid data. Temporary output cannot remain available after the connection closes; held batches require a fresh approved extraction after resolution. In scheduled runs, a batch held for MANUAL REVIEW or REJECTED marks the Cron job as failed so the operator is alerted.

Stage 2 does not retry within a run. A database error during transformation rejects the run with `TRANSFORMATION_FAILED`. The next scheduled run, or a manual rerun, starts again from extraction with new run IDs. Business and validation failures are never retried automatically; they are corrected at the source first.

Handover includes:

- The temporary output reference (`pg_temp.transformed_daily_branch_summary`)
- The transformation run ID, and the parent extraction run ID with its run-start timestamp (`extraction_started_at`, passed through unchanged)
- The batch ID and cutoff. The handover builds the batch window from these ([Load Stage, Section 1](../Load/Load.md#load-input)).
- Schema, rule, and code versions
- The output count, the row-accounting and quarantine counts, validation results, errors, and the acceptance status

Load acknowledges the run ID and approved decision while the output is available in the active connection.

Record NOT_CONFIGURED when no Load receiver exists. Successful transformation alone is never recorded as successful publication. Load owns permanent writes and publication acceptance, in its own publication transaction (Section 1).

Persistent log-delivery failure is an operational failure recorded separately under Section 8. It does not retroactively change calculated validation results or authorize replay of a completed Load operation. Recover undelivered logs before treating the run as operationally complete.

## 11. Common Transformation Problems and Mitigation

| Problem | Possible cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Staging unavailable | Transformation starts in a separate session or after extraction commits | Batch cannot consume its approved inputs | Invoke through the active handover; restart extraction after session loss |
| Inflated measures | Raw orders, payments, and expenses joined before grouping | Duplicated order value, collections, or expenses | Aggregate streams independently; verify join cardinality and group totals |
| Wrong business date | UTC calendar date used directly or cutoff recalculated mid-run | Activity assigned to incorrect days | Apply Manila conversion and one captured cutoff; preserve expense DATE |
| Missing historical changes | Only recent orders processed | Late payments or corrections absent from summaries | Rebuild completed-day history from full staged input |
| Unknown status | Application introduces a code absent from approved mapping | Unclear eligibility or released-order counts | Quarantine the affected orders (S-03); if the code is legitimate, version the mapping and rerun; the Section 10 rate holds the batch if the code is widespread |
| Negative amount or cross-branch collection | Refund/adjustment or collection practice not defined | Ambiguous financial attribution | Quarantine the rows (C-07, C-08) for owner review; correct them with `python -m pipeline.quarantine --fix` or approve an explicit policy, never guessing repairs |
| Overflow or rounding discrepancy | Large totals or per-row rounding | Invalid target values or reconciliation failure | Sum exact NUMERIC values, round once per group, and validate target range |
| Different results on rerun | Source corrections, changed statuses, or changed rules | Historical metrics change | Compare extraction run IDs, cutoff, and rule versions; verify identical-input determinism |
| Empty completed-day output | No eligible history or unintended exclusions | Full-replacement loading could clear reports | Hold for review; verify exclusion counts and cutoff before approval |
| Widespread defects | Many source records break the cleaning rules | Published totals would understate activity and mislead branch comparisons | Hold above 5% and reject above 10% per stream (Section 10); fix the listed records with `python -m pipeline.quarantine --fix` and run again |
| Missing failure diagnostics | Connection ends before durable logging | Batch cannot be fully traced | Use pending metadata and replay; report incomplete evidence and inspect scheduler history |
| Full-history processing slows | Source history grows | Transformation delays the daily pipeline | Monitor duration and contributing counts; review SQL plans and reassess batch design with reconciliation tests |
| Permanent publication blocked | Load writes the target inside the read-only extraction transaction | `cannot execute INSERT in a read-only transaction`; nothing published | Keep Stage 2 output temporary; Load copies it during the snapshot and publishes in a separate read-write transaction in the same session ([Load Stage, Section 1](../Load/Load.md#transaction-design)) |

Transformation must stop handover when mitigation requires a change to business rules, input contracts, or transaction design. Record the cause and applied correction, then validate a new batch before publication.

## Expected Output

One temporary table, `pg_temp.transformed_daily_branch_summary`, holds one validated row per completed business date and branch, following the Section 6 contract. It is handed to Load in the same session with its run metadata, quarantine counts and rates, validation results, and acceptance decision. Every input row is accounted for as contributing, excluded, deferred, or quarantined. Quarantined rows are logged with their rule and reason and listed for correction. Source and staged records are never changed, and no permanent reporting table is written in this stage.
