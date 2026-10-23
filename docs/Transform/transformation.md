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

The handover must include the extraction `pipeline_run_id`, run-start timestamp, staged counts, validation results, and `APPROVED` acceptance status. Transformation must acknowledge the extraction run ID and acceptance decision before consuming the staged records. Batches marked `MANUAL REVIEW` or `REJECTED` must not proceed.

The received batch contains all available history. Transformation must use the extraction run-start timestamp to establish one reporting cutoff: the start of that run's calendar date in **Asia/Manila**. Only business dates earlier than this cutoff contribute to completed-day reporting. Current-day and future-dated records must be counted separately from reporting contributions.

Transformation must read the supplied staging tables, not re-extract records from the source system.

### Batch processing method

SQL will perform profiling, cleaning, standardization, business calculations, and validation inside PostgreSQL. Records must remain in the database. Transformation must use working copies or derived temporary outputs while preserving the raw extraction stages. Create empty temporary working and output tables during the setup transaction before the read-only extraction snapshot begins; populate and update them within the shared snapshot. Do not create these tables inside the read-only transaction.

The batch must run daily immediately after approved extraction in the **01:00 Asia/Manila** pipeline execution. It must use the active extraction handover rather than a separately scheduled connection, because temporary staging is session-local.

Each batch must process the full staged history and rebuild the eligible completed-day measures. This allows subsequent batches to incorporate historical corrections and late collections. Orders use their creation dates, payments use their collection dates, and expenses use their recorded expense dates.

If transformation fails, stop handover to Load and discard incomplete transformed output. After the transaction ends, restart through the Extraction Stage to obtain a new approved snapshot and extraction run ID. Transformation must not attempt to reuse temporary tables from a closed connection.

Reruns must be idempotent: the same staged input, reporting cutoff, and transformation rules must produce the same business rows without appending duplicates. A new extraction snapshot may legitimately change historical results when source records have been corrected.

### Transformation scope

| Input | Fields processed | Transformation scope |
| --- | --- | --- |
| `stage_branches` | `id`, `branch_name` | Prepare consistent branch labels and provide branch lookups. |
| `stage_customers` | `id` | Provide customer lookups without adding personal information. |
| `stage_orders` | `id`, `order_number`, `branch_id`, `customer_id`, `created_at`, `status`, `weight_kg`, `total_price` | Retain identifiers for traceability; standardize reporting dates and statuses; apply documented eligibility rules; calculate order counts, released-order counts, distinct customer counts, total weight, and order value. |
| `stage_payments` | `id`, `order_id`, `branch_id`, `paid_at`, `amount` | Retain payment and order references; standardize collection dates; calculate payment counts and cash received. |
| `stage_expenses` | `id`, `branch_id`, `expense_date`, `amount`, `deleted_at` | Retain expense references and dates; exclude soft-deleted expenses with recorded counts; calculate expense totals. |

Orders, payments, and expenses must be aggregated independently by business date and branch before their results are combined. The resulting dataset must contain one row per reporting date and branch for handover to Load. Detailed cleaning, standardization, and business rules will be defined in the corresponding transformation sections.

### Dependencies and assumptions

- Process branch and customer reference lookups before the related transaction datasets. Resolve payment order references against staged orders before aggregating payments.
- Required staging tables, selected columns, and extraction metadata must match the Stage 1 handover contract. Missing staging or unavailable lookups stop the batch; transformation must not fetch replacement source data.
- Required branch assignments and reporting dates must be resolved before reporting output is approved. Transformation must not silently assign a default branch or date.
- Convert order and payment timestamps to Asia/Manila business dates. Preserve `expense_date` as its existing calendar date.
- Monetary values represent PHP and weights represent kilograms. Use exact PostgreSQL `NUMERIC` arithmetic for calculations.
- Full-history processing includes late payments and historical corrections in subsequent runs. Its workload grows with the staged history.
- Temporary staging and transformed output must be consumed before the active transaction and connection end. The existing extraction transaction permits writes to temporary tables but is read-only for permanent tables; permanent publication requires a compatible transaction design when integrating the Load Stage.

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

## 3. Data Cleaning Rules

Cleaning must operate on temporary working copies and preserve raw staging. The profiling categories in Section 2 identify the targets below; measured findings remain pending. Rule counts must distinguish changed, rejected, and retained rows.

| Rule ID | Target | Condition | Action | Justification |
| --- | --- | --- | --- | --- |
| C-01 | Branch labels and order statuses | Leading or trailing whitespace | Trim working values; count changed rows | Prevent formatting differences from affecting labels or status mapping |
| C-02 | Required labels and statuses | NULL, blank after trimming, or embedded control characters | Reject and log; stop batch approval | No reliable replacement can be inferred |
| C-03 | Weight and monetary fields | NULL or non-finite numeric value | Reject and log | These values cannot support valid reporting totals |
| C-04 | Order weight | Negative value | Reject and log; stop batch approval | Negative weight cannot represent laundry demand |
| C-05 | IDs and order numbers | Duplicate identifier in working data | Reject the batch; retain neither row as an arbitrary winner | No extracted update timestamp establishes which version is authoritative |
| C-06 | Required references and dates | Missing value or unmatched staged parent | Reject and log; do not substitute a default | Prevent incorrect branch, customer, order, or date attribution |
| C-07 | Payment branch versus order branch | Different valid branch IDs | Hold for review without overwriting either value | Cross-branch collection handling requires an approved business rule |
| C-08 | Order value and expense amount | Negative value | Hold the batch for review; reject if no approved business rule supports the value | Refund and adjustment semantics must be defined before negative amounts contribute to reporting |
| C-09 | Payment amount | Zero or negative value | Reject and log | The source schema requires payment amount greater than zero |

Duplicate detection must use identifiers, not repeated amounts or payment order references. Multiple payments for one order are valid distinct records when payment IDs differ. No automatic duplicate removal is permitted without a documented selection rule.

Unusually large finite values must be reviewed using measured profiles; no unverified threshold may silently remove them. Business exclusions such as deleted expenses belong to Section 5.

Each rejected row must be counted once, with all applicable rule IDs retained. Modified rows remain part of retained input. Missing values must not be replaced with zero merely to make calculations succeed. Link actual profiling findings to these rules when Section 2 measurements are available.

## 4. Standardization and Data Type Rules

Standardization must preserve business meaning and operate on the cleaned working datasets.

| Rule ID | Target | Standard and conversion | On failure |
| --- | --- | --- | --- |
| S-01 | Order and payment timestamps | Derive DATE using `(timestamp_value AT TIME ZONE 'Asia/Manila')::date`; retain original TIMESTAMPTZ for traceability | Reject missing or non-finite required dates |
| S-02 | Expense date | Preserve DATE without timezone conversion; display as YYYY-MM-DD | Reject invalid required date |
| S-03 | Order status | Lowercase trimmed text; validate against a versioned approved status mapping | Hold unknown codes for review; do not guess their meaning |
| S-04 | Branch label | Preserve spelling and case after C-01 trimming | Reject unusable labels under C-02 |
| S-05 | Weight and amounts | Use exact NUMERIC for intermediate calculations; round each final group total once to two decimals for NUMERIC(14,2) | Reject non-finite values or totals outside the target range |
| S-06 | Identifiers and counts | Retain UUID identifiers; use BIGINT counts | Reject incompatible types or overflow |
| S-07 | Run timestamps | Preserve TIMESTAMPTZ; display ISO 8601 with timezone offset | Reject unparseable required metadata |

Currency is PHP and weight is kilograms. No currency or unit conversion is performed. Numeric source values are already typed; do not convert through floating-point arithmetic or formatted currency strings.

Rule version `1.0.0` recognizes the source schema's codes `received`, `on_process`, `ready`, `released`, and `cancelled`. The first four contribute to eligible order measures; `released` also contributes to released-order counts. Cancelled orders are excluded from order measures. Case and whitespace normalization does not authorize mapping unfamiliar codes to existing statuses. Unknown statuses hold the batch for review. Record the mapping version and changed-row counts.

Rounding must occur after aggregation, not once per source row. Validate the rounded result before casting to NUMERIC(14,2). Conversion failures must be logged with the rule ID and affected record or group; they must not become silent NULLs or zeroes.

## 5. Business Transformation Rules

Transform eligible working records into one daily branch summary. Reuse the captured reporting cutoff for the entire batch.

| Rule ID | Input | Logic | Output / purpose |
| --- | --- | --- | --- |
| B-01 | Order and payment timestamps; expense dates | Use S-01/S-02 business dates; retain only dates earlier than the cutoff | Completed-day reporting; count deferred rows separately |
| B-02 | Standardized order status | Exclude orders mapped to the approved cancelled code; record count and reason | Eligible order activity and value |
| B-03 | Expense deletion timestamp | Exclude expenses with non-NULL deleted_at | Eligible expense totals |
| B-04 | Eligible orders | Group by order business date and branch_id | order_count, released_order_count, unique_customer_count, total_weight_kg, order_value_php |
| B-05 | Eligible payments | Group by collection business date and payment branch_id | payment_count and cash_received_php |
| B-06 | Eligible expenses | Group by expense_date and branch_id | expense_total_php |
| B-07 | Three aggregate datasets | Union distinct date/branch keys, then left join each aggregate on both keys | One row per date and branch; zero for an absent stream |
| B-08 | Staged branch/customer/order references | Resolve UUID lookups with exactly one matching parent | Stable relationships without multiplying detail rows |

For B-04, count eligible orders, count those mapped to released, count distinct customer_id, and sum weight_kg and total_price. Released orders are grouped by creation date; no release-event timestamp is available. A status change can therefore revise historical results.

Payments remain independent ledger entries. A payment on a cancelled order must not be discarded solely because that order is excluded from order metrics. Negative amounts and cross-branch collections cannot proceed until the review requirements in Section 3 are resolved. Cancellation and release mappings must be approved before deployment; unknown statuses must never be treated implicitly as eligible.

Never join raw orders, payments, and expenses together for aggregation. Each stream must first produce unique date/branch keys. A day with payments or expenses but no eligible orders must remain in the output. A date/branch with no eligible activity in any stream has no output row.

No surrogate keys, dimension tables, or slowly changing dimensions are introduced. Branch IDs remain the grouping keys; labels come from staged branch references.

Assign each input row one accounting category in this order: rejected, business-excluded, deferred, or contributing. Record additional matched reasons separately without double-counting exclusions. Preserve counts and source identifiers for traceability.

## 6. Transformed Data Contract and Naming Convention

The Load Stage receives `pg_temp.transformed_daily_branch_summary` in the active connection. All fields below are NOT NULL. The composite key is `(report_date, branch_id)`; branch IDs must resolve to staged branches.

| Output column | Type | Source-to-target mapping |
| --- | --- | --- |
| report_date | DATE | Union of eligible order, payment, and expense business dates |
| branch_id | UUID | Branch key from each independent aggregate |
| order_count | BIGINT | Count of B-04 eligible orders, otherwise 0 |
| released_order_count | BIGINT | B-04 count of eligible released orders, otherwise 0 |
| unique_customer_count | BIGINT | B-04 distinct customer IDs per group, otherwise 0 |
| total_weight_kg | NUMERIC(14,2) | Rounded B-04 sum of weight_kg, otherwise 0 |
| order_value_php | NUMERIC(14,2) | Rounded B-04 sum of total_price, otherwise 0 |
| payment_count | BIGINT | Count of B-05 payments, otherwise 0 |
| cash_received_php | NUMERIC(14,2) | Rounded B-05 sum of amount, otherwise 0 |
| expense_total_php | NUMERIC(14,2) | Rounded B-06 sum of amount, otherwise 0 |

Use lowercase snake_case for tables and columns. Retain branch_id as the UUID key; do not introduce a branch surrogate key. Validate uniqueness and required values before handover.

The permanent destination remains `analytics.daily_branch_summary`. The Load Stage supplies `refreshed_at` when publication succeeds; transformation completion time is recorded separately in metadata.

Input schema must match the Stage 1 contract. Missing required fields or incompatible types stop the batch. Additional unused fields do not alter transformation output. Version any intentional input mapping or output schema change and verify Load compatibility before deployment.

## 7. Transformation Validation and Data Quality Checks

Validation must assess transformed data and its contributing staged records. It must not repeat source connectivity or extraction execution checks.

| Check | Target and purpose | Passing criterion | Severity |
| --- | --- | --- | --- |
| Row accounting | Each transaction stream; detect unexplained losses | received = rejected + business_excluded + deferred + contributing | Critical |
| Reference accounting | Branch/customer working rows | received = retained + rejected | Critical |
| Join cardinality | Fact lookups and aggregate joins | Required lookup matches exactly once; joins do not multiply rows | Critical |
| Output key coverage | Combined aggregate | Keys equal the union of eligible stream keys; no duplicates | Critical |
| Required fields and types | Output contract | No NULL required fields; all values conform to Section 6 | Critical |
| Branch integrity | Output branch_id | Every branch resolves to exactly one staged branch | Critical |
| Reporting window | report_date | Every date precedes the captured cutoff | Critical |
| Count reconciliation | Order and payment counts | Per-group counts equal contributing inputs; released and distinct customer counts recompute correctly | Critical |
| Measure reconciliation | Weight, order value, collections, expenses | Each group equals its exact contributing sum rounded under S-05 | Critical |
| Count relationships | Order measures | released_order_count and unique_customer_count do not exceed order_count | Critical |
| Numeric validity | Output measures | Counts and weight are nonnegative; all measures are finite and within range; monetary signs comply with the approved rule version | Critical |
| Policy review | Status, monetary signs, and branch attribution | No unresolved status mapping, negative-amount policy, or cross-branch decision | Warning; blocks automatic handover |
| Empty reporting output | Full-history batch | Empty eligible history explicitly reviewed before replacement loading | Warning; blocks automatic handover |

Modified retained rows are not an extra term in the row-accounting equation. A row can match several rules but belongs to only one final accounting category.

Aggregate row count is not expected to equal input row count. Reconcile contributing rows and measures per stream and per date/branch. Distinct customer counts must be compared within each group rather than against one global distinct count.

Compare rounded group sums with the same per-group rounding method. The sum of rounded groups may differ from a rounded grand total; record the difference without hiding a failed group check.

Any critical failure rejects the batch. Warnings hold the batch for review under Section 10. Record expected values, actual values, affected counts, and check outcomes.

## 8. Transformation Metadata and Log Specification

Store transformation metadata in restricted `etl_ops.transformation_logs`. Keep transformation and extraction run identities distinct and linked. Each table-level entry must identify its grain so aggregate output counts are not confused with contributing source counts.

| Field | Purpose and format |
| --- | --- |
| run_id | Transformation UUID |
| parent_run_id | Extraction pipeline_run_id; references Stage 1 metadata |
| retry_of_run_id | Previous transformation attempt UUID, nullable |
| batch_id | Logical batch identifier associated with cutoff and rules |
| batch_window | Earliest processed business date and exclusive reporting cutoff; NULL lower date for empty input |
| table_name | Input stream or transformed output name |
| transformation_started_at, transformation_ended_at | TIMESTAMPTZ execution boundaries |
| transformation_status | PASS, WARNING, or FAIL |
| acceptance_status | APPROVED, MANUAL REVIEW, or REJECTED |
| log_delivery_status | PENDING, SAVED, or FAILED; separate from the data acceptance decision |
| records_read, records_output | Received and produced row counts at the stated grain |
| records_rejected | Distinct rejected rows |
| records_excluded, records_deferred, records_contributing | Exclusive row-accounting totals |
| records_modified | Distinct changed and retained rows |
| rules_applied | JSONB rule IDs and affected counts; rule counts may overlap |
| validation_results | JSONB check outcomes with expected and actual values |
| error_code, error_message | Sanitized failure category and description |
| duration_ms | Nonnegative runtime |
| code_version, rule_version, schema_version | Implementation and contract versions used by the batch |

Use metadata for monitoring, rule impact analysis, lineage, troubleshooting, and comparison of corrected reruns. Do not duplicate all extraction metadata or store credentials in diagnostics.

Failure evidence must survive rollback. The orchestration layer must capture diagnostics before ending the processing transaction and persist them through a separate authorized logging transaction afterward. Permanent writes must not be attempted inside the existing read-only extraction transaction.

Undelivered metadata must remain in a restricted pending store until delivery commits. Replay must preserve original run IDs and timestamps and avoid duplicate entries. If a connection fails before diagnostic evidence is durably captured, record incomplete logging and inspect available scheduler/service evidence; do not claim guaranteed recovery.

Keep data acceptance separate from log-delivery status. Validation determines whether temporary output may be handed over while the transaction is active. Final-log persistence is confirmed after that transaction ends. Record log delivery as PENDING, SAVED, or FAILED, and do not report the run as fully logged until delivery is SAVED. A logging failure must not trigger an automatic repeat of a downstream operation that may already have completed.

## 9. Log Retention, Rejected Data, and Access

Transformation logs and rejection evidence must be retained for at least one year to investigate historical report changes and failed batches. Longer organizational audit requirements take precedence.

| Record | Storage | Contents and ownership |
| --- | --- | --- |
| Transformation logs | etl_ops.transformation_logs | Counts, rule actions, validation outcomes, decisions, and lineage; owned by authorized ETL operations role |
| Rejection evidence | etl_ops.transformation_quarantine | Batch/run IDs, staged table, record UUID, rule IDs, reason, and minimal offending values; owned by authorized ETL operations role |
| Pending metadata | Restricted orchestration pending store | Undelivered diagnostic metadata; retained until durable delivery is confirmed |

Quarantine must contain only the fields needed to diagnose a failed transformation. Do not copy complete customer records or unnecessary sensitive data. Business-excluded and deferred records are not quarantined as invalid records; record their counts and reasons separately.

Quarantine is not a substitute for extraction staging. After correction, rerun extraction and transformation against a new approved snapshot. Retain rejection evidence even when the failed processing transaction rolls back through the logging arrangement in Section 8.

The runtime may write through authorized logging paths. Approved operators and administrators may read. Only authorized administrators may archive or delete. Enforce restricted database privileges and appropriate row-level policies before deployment.

Archive older records within restricted PostgreSQL storage when needed. Verify counts, run IDs, reasons, and timestamps before treating an archive as complete. Archive copies remain subject to the original retention and access requirements.

Before deletion, an administrator must confirm that the retention period has elapsed and the records are not needed for troubleshooting, audit, or recovery. Record the approval, run IDs, expected and actual counts, and outcome in a separate maintenance audit record. Pending entries must not be removed before successful delivery. Audit records follow their own applicable retention period.

## 10. Transformation Acceptance and Handover Rules

The rejection-rate threshold is **0%**, defined before processing. Daily financial summaries must not silently omit invalid contributing records. Business exclusions and date-window deferrals are not rejections.

Calculate the rejection rate from distinct rejected transaction rows divided by received transaction rows. For an empty denominator, record the rate as not applicable and apply the empty-output review rule. Reference failures independently block acceptance.

| Validation condition | Acceptance | Pipeline action | Decision and responsibility |
| --- | --- | --- | --- |
| All critical checks pass; zero rejected rows; no unresolved warnings; required validation and handover metadata captured | APPROVED | Offer validated output to Load; finalize persistent logging after the transaction ends | Automatic |
| No critical failure, but unresolved status, negative-amount, or branch policy, or an empty-output warning | MANUAL REVIEW | Hold handover; preserve evidence | ETL operator and data owner review |
| Any critical failure, rejected row, incompatible contract, or missing required validation or handover metadata | REJECTED | Stop handover and discard incomplete output | Automatic stop; operator resolves the cause |

Manual review must resolve the issue or establish a versioned approved policy before a new batch is evaluated. Review does not override failed reconciliation or convert an unsupported value into valid data. Temporary output cannot remain available after the connection closes; held batches require a fresh approved extraction after resolution.

Retry transient operational failures with a new attempt and linked run IDs. Do not automatically retry unresolved business or validation failures.

Handover must include the temporary output reference, transformation run ID, parent extraction run ID, batch window and cutoff, schema/rule/code versions, output count, rejection/exclusion/contribution counts, validation results, errors, and acceptance status. Load must acknowledge the run ID and approved decision while the output is available in the active connection.

Record NOT_CONFIGURED when no Load receiver exists. Successful transformation alone must not be recorded as successful publication. Load owns permanent writes and publication acceptance; integration must resolve the transaction dependency identified in Section 1.

Persistent log-delivery failure is an operational failure recorded separately under Section 8. It does not retroactively change calculated validation results or authorize replay of a completed Load operation. Recover undelivered logs before treating the run as operationally complete.

## 11. Common Transformation Problems and Mitigation

| Problem | Possible cause | Impact | Mitigation |
| --- | --- | --- | --- |
| Staging unavailable | Transformation starts in a separate session or after extraction commits | Batch cannot consume its approved inputs | Invoke through the active handover; restart extraction after session loss |
| Inflated measures | Raw orders, payments, and expenses joined before grouping | Duplicated order value, collections, or expenses | Aggregate streams independently; verify join cardinality and group totals |
| Wrong business date | UTC calendar date used directly or cutoff recalculated mid-run | Activity assigned to incorrect days | Apply Manila conversion and one captured cutoff; preserve expense DATE |
| Missing historical changes | Only recent orders processed | Late payments or corrections absent from summaries | Rebuild completed-day history from full staged input |
| Unknown status | Application introduces a code absent from approved mapping | Unclear eligibility or released-order counts | Hold batch, review observed values, version mapping, and rerun |
| Negative amount or cross-branch collection | Refund/adjustment or collection practice not defined | Ambiguous financial attribution | Hold for owner review; establish explicit policy without guessing repairs |
| Overflow or rounding discrepancy | Large totals or per-row rounding | Invalid target values or reconciliation failure | Sum exact NUMERIC values, round once per group, and validate target range |
| Different results on rerun | Source corrections, changed statuses, or changed rules | Historical metrics change | Compare extraction run IDs, cutoff, and rule versions; verify identical-input determinism |
| Empty completed-day output | No eligible history or unintended exclusions | Full-replacement loading could clear reports | Hold for review; verify exclusion counts and cutoff before approval |
| Missing failure diagnostics | Connection ends before durable logging | Batch cannot be fully traced | Use pending metadata and replay; report incomplete evidence and inspect scheduler history |
| Full-history processing slows | Source history grows | Transformation delays the daily pipeline | Monitor duration and contributing counts; review SQL plans and reassess batch design with reconciliation tests |
| Permanent publication blocked | Existing extraction transaction is read-only | Load cannot write the target | Resolve orchestration transaction and role privileges before integrating Load; keep Stage 2 output temporary |

Transformation must stop handover when mitigation requires a change to business rules, input contracts, or transaction design. Record the cause and applied correction, then validate a new batch before publication.
