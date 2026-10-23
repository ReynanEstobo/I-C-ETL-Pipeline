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
