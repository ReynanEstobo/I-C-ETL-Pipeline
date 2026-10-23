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

SQL will perform profiling, cleaning, standardization, business calculations, and validation inside PostgreSQL. Records must remain in the database. Transformation must use working copies or derived temporary outputs while preserving the raw extraction stages.

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

The following register defines the transformation profiling checks. Measurements are pending. Example values describe the check conditions and are not recorded findings. Completed profiles must include measured counts and sanitized observed values.

| Table and column | Issue or characteristic to inspect | Illustrative sample | Affected records | Planned treatment |
| --- | --- | --- | --- | --- |
| `stage_branches.branch_name` | NULL or blank labels, leading/trailing whitespace, and control characters | ` Main `, empty text | Pending measurement | Section 3: define label cleaning and treatment of unusable labels. |
| `stage_orders.status` | NULL or blank statuses, inconsistent case/spacing, and distinct status values | `Released`, ` released ` | Pending measurement | Section 4: define standard values from the observed vocabulary and approved business meanings. |
| `stage_orders.weight_kg`, `stage_orders.total_price` | NULL, negative, non-finite, or unusually large values | `-1`, `NaN` | Pending measurement | Section 3: define invalid-value handling; Section 4: define numeric precision. Review unusual finite values before classifying them as errors. |
| `stage_payments.amount`, `stage_expenses.amount` | NULL, negative, non-finite, or unusually large amounts | `-25`, `Infinity` | Pending measurement | Section 3: establish valid amount ranges and treatment with the data owner; do not assume a negative amount is an authorized refund. |
| `stage_orders.created_at`, `stage_payments.paid_at`, `stage_expenses.expense_date` | Earliest/latest business dates and counts before, on, or after the reporting cutoff | Business date equal to cutoff | Pending measurement | Section 4: define Manila date standardization; Section 5: apply the completed-day reporting window and count deferred records. |
| `stage_orders.status` | Number of cancelled orders and their associated values | `cancelled`, if present | Pending measurement | Section 5: define order eligibility and record business exclusions. Confirm the actual cancellation code and policy first. |
| `stage_expenses.deleted_at` | Number and amount of soft-deleted expenses | Non-NULL deletion timestamp | Pending measurement | Section 5: exclude soft-deleted expenses from reporting and record the excluded count. |
| `stage_payments.branch_id`, linked `stage_orders.branch_id` | Different valid branches on a payment and its order | Two different branch IDs | Pending measurement | Section 5: document collection attribution after reviewing whether cross-branch payments are valid business activity. |
| `stage_payments.order_id` | Distribution of payment rows per order | Two payments linked to one order | Pending measurement | Section 5: aggregate payments independently; repeated order references are not duplicate payment records. |

Business exclusions, date-window deferrals, and multiple payments per order are distinct from data quality errors. Profiling must preserve all staged records. Each observed issue must reference its cleaning, standardization, or business rule in Sections 3, 4, or 5. A completed profile must contain measured findings and planned treatments for an identified batch.
