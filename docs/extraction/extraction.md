# I&C Laundry Daily Branch Reporting ETL

## Stage 1 – Data Extraction

### Objective

The objective of this stage is to extract data from the identified source system and prepare a documented, traceable, and validated dataset for the next stage of the ETL pipeline.

---

## 1. Data Source and Extraction Specification

### Source System

The source system is the shared Supabase PostgreSQL database used by I&C Laundry for its Main, Calzada, and Nasugbu branches.

The database stores operational records such as laundry orders, payments, customers, expenses, and branch information. These records are used as the source for daily branch reporting.

### Source Database and DBMS

- **Database:** I&C Laundry shared database
- **DBMS:** PostgreSQL
- **Hosting:** Supabase
- **Schema:** `public`

### Extraction Method

The extraction uses SQL `SELECT` queries through an authorized read-only database connection.

The extraction is performed as a full-history extraction. All available records from the selected source tables are extracted during each run. No incremental field or watermark is used.

The five source tables are read from a consistent PostgreSQL snapshot. The tables are extracted separately without joins so that the original source row structure is preserved.

### Extraction Technology

- PostgreSQL
- Supabase
- SQL
- Supabase Cron / PostgreSQL scheduling

### Extraction Schedule

The extraction is scheduled daily at **01:00 Asia/Manila time**.

The equivalent UTC cron schedule is:

`0 17 * * *`

### Extraction Scope

The extraction includes the following source tables:

- `public.branches`
- `public.customers`
- `public.orders`
- `public.payments`
- `public.expenses`

Only the columns required for the reporting process are extracted.

Source values, data types, and NULL values are preserved during extraction.

Cancelled orders, soft-deleted expenses, records with missing dates, and records with missing branch assignments are not removed during extraction. These records are retained for validation and are handled by later stages when applicable.

### Source Limitations and Assumptions

The extraction is based on the source schema provided for the project.

The following conditions are considered during extraction:

- Some branch, customer, or order date fields may contain NULL values.
- Historical records may be corrected after their original creation.
- Payments may be recorded after an order is created.
- Source relationships are expected to follow the defined foreign keys.
- The extraction uses the Asia/Manila time zone for the daily schedule.
- Full-history extraction may increase in size as more records are added.

---

## 2. Source Tables and Column Specification

### `public.branches`

**Purpose:** Stores branch information used to identify the laundry branches.

| Column        | Type | Description                     |
| ------------- | ---- | ------------------------------- |
| `id`          | UUID | Unique identifier of the branch |
| `branch_name` | TEXT | Branch label used for reporting |
| `name`        | TEXT | Branch name                     |
| `code`        | TEXT | Branch code                     |

**Primary Key:** `id`

**Relationships:** Referenced by branch IDs in orders, payments, and expenses.

**Reason for Inclusion:** Provides branch identification and labels needed for branch-level reporting.

---

### `public.customers`

**Purpose:** Stores customer records used to determine customer activity.

| Column | Type | Description                       |
| ------ | ---- | --------------------------------- |
| `id`   | UUID | Unique identifier of the customer |

**Primary Key:** `id`

**Relationships:** Referenced by `orders.customer_id`.

**Reason for Inclusion:** Provides customer identifiers needed for customer-related reporting and unique customer counts.

---

### `public.orders`

**Purpose:** Stores laundry order transactions.

| Column        | Type        | Description                              |
| ------------- | ----------- | ---------------------------------------- |
| `id`          | UUID        | Unique identifier of the order           |
| `branch_id`   | UUID        | Branch associated with the order         |
| `customer_id` | UUID        | Customer associated with the order       |
| `created_at`  | TIMESTAMPTZ | Date and time when the order was created |
| `status`      | TEXT        | Current status of the order              |
| `weight_kg`   | NUMERIC     | Laundry weight in kilograms              |
| `total_price` | NUMERIC     | Total value of the order                 |

**Primary Key:** `id`

**Relationships:** References `branches.id` and `customers.id`.

**Reason for Inclusion:** Provides order counts, order status, customer activity, laundry weight, demand information, and order value.

All order statuses are retained during extraction.

---

### `public.payments`

**Purpose:** Stores payment records associated with laundry orders.

| Column      | Type        | Description                             |
| ----------- | ----------- | --------------------------------------- |
| `id`        | UUID        | Unique identifier of the payment        |
| `order_id`  | UUID        | Order associated with the payment       |
| `branch_id` | UUID        | Branch associated with the payment      |
| `amount`    | NUMERIC     | Payment amount                          |
| `paid_at`   | TIMESTAMPTZ | Date and time when payment was received |
| `source`    | TEXT        | Payment source                          |

**Primary Key:** `id`

**Relationships:** References `orders.id` and `branches.id`.

**Reason for Inclusion:** Provides payment records and cash collection information for reporting.

---

### `public.expenses`

**Purpose:** Stores branch expense records.

| Column         | Type        | Description                                    |
| -------------- | ----------- | ---------------------------------------------- |
| `id`           | UUID        | Unique identifier of the expense               |
| `branch_id`    | UUID        | Branch associated with the expense             |
| `amount`       | NUMERIC     | Expense amount                                 |
| `expense_date` | DATE        | Date of the expense                            |
| `deleted_at`   | TIMESTAMPTZ | Indicates whether the expense was soft-deleted |

**Primary Key:** `id`

**Relationships:** References `branches.id`.

**Reason for Inclusion:** Provides expense information needed for branch reporting.

Soft-deleted expenses are retained during extraction.

---

### Row-Level Extraction

Each extracted row represents one row from its corresponding source table.

The five tables are preserved separately during extraction. No one-to-many joins are performed during this stage to avoid changing the original row structure or multiplying records.

---

## 3. Extraction Validation and Data Quality Checks

The following checks are performed at the extraction level to ensure that the extracted data is complete, reliable, and suitable for the next ETL stage.

| Check Name                    | Target                                 | Purpose                                             | Validation Criteria                                |
| ----------------------------- | -------------------------------------- | --------------------------------------------------- | -------------------------------------------------- |
| Source Connection Check       | Database connection                    | Confirm that the source database can be accessed    | Connection is successful                           |
| Required Table Check          | Five source tables                     | Confirm that all required tables are available      | All required tables exist                          |
| Required Column Check         | Selected source columns                | Confirm that required columns are available         | All required columns exist                         |
| Required ID Check             | Primary and required identifiers       | Check for missing required identifiers              | Required IDs are not NULL                          |
| Primary Key Uniqueness        | Primary key columns                    | Detect duplicate source records                     | Primary key values are unique                      |
| Foreign Key Check             | Branch, customer, and order references | Check source relationships                          | References are valid or reported as warnings       |
| Record Count Check            | Each source table                      | Confirm that records were completely extracted      | Extracted count matches the source count           |
| Date Availability Check       | Date and timestamp columns             | Identify missing dates needed for reporting         | Missing dates are recorded as validation warnings  |
| Extraction Interruption Check | Extraction process                     | Detect incomplete extraction                        | Extraction completes successfully                  |
| Snapshot Consistency Check    | Extracted source tables                | Ensure the tables belong to the same extraction run | Tables are extracted from the same source snapshot |
| Value Serialization Check     | Extracted values                       | Confirm that source values can be stored correctly  | Values and data types are preserved                |

Validation results are recorded with the extraction run.

Warnings do not automatically remove or modify source records. Cleaning, standardization, filtering, and business transformations are handled in the Transformation Stage.

---

## 4. Extraction Metadata and Log Specification

The extraction process records metadata for every run.

| Metadata / Log Field    | Purpose                                               |
| ----------------------- | ----------------------------------------------------- |
| `pipeline_run_id`       | Identifies a specific extraction run                  |
| `pipeline_name`         | Identifies the ETL pipeline                           |
| `source_system`         | Identifies the source database                        |
| `table_name`            | Identifies the extracted table                        |
| `snapshot_started_at`   | Records when the source snapshot started              |
| `extraction_started_at` | Records the extraction start time                     |
| `extraction_ended_at`   | Records the extraction end time                       |
| `extraction_status`     | Records whether the extraction succeeded or failed    |
| `records_read`          | Records the number of records read                    |
| `records_extracted`     | Records the number of records successfully extracted  |
| `records_rejected`      | Records the number of rejected records, if applicable |
| `extraction_window`     | Records the extraction date or range                  |
| `validation_results`    | Records the results of validation checks              |
| `error_code`            | Identifies an extraction error                        |
| `error_message`         | Describes the extraction error                        |
| `duration_ms`           | Records the extraction duration                       |
| `output_reference`      | Identifies the extracted output                       |
| `source_row_count`      | Records the source row count                          |
| `output_checksum`       | Helps verify the extracted output                     |

### Purpose of Metadata and Logs

The metadata and logs are used for:

- Monitoring extraction runs
- Troubleshooting extraction problems
- Auditing extraction activities
- Tracking extraction history
- Supporting recovery when an extraction fails

A failed retry receives a new pipeline run ID so that each run can be tracked separately.

---

## 5. Log Retention and Access

### Retention Period

Extraction logs are retained for at least **one year**.

This provides sufficient history for monitoring, troubleshooting, auditing, and reviewing previous extraction runs.

### Storage

Metadata and extraction logs are stored separately from the raw extracted datasets.

### Access

Access to extraction metadata and logs is restricted to authorized personnel responsible for the ETL pipeline and system operations.

### Archiving

Older logs may be archived when necessary while maintaining their required retention period.

### Deletion

Logs are deleted only after the retention period has ended and there is no remaining operational or audit requirement for them.

Operational records and audit records are treated separately when applicable.

---

## 6. Extraction Data Contract and Naming Convention

The extraction data contract defines the expected structure of the data handed to the next ETL stage.

### Expected Source Tables

The extraction package contains five separate datasets:

- `branches`
- `customers`
- `orders`
- `payments`
- `expenses`

### Naming Convention

The following naming conventions are used:

- Lowercase `snake_case` for table and column names
- Clear and consistent names
- No unnecessary abbreviations
- Primary keys use `id`
- Foreign keys use the related table name followed by `_id`, such as `branch_id`, `customer_id`, and `order_id`

### Data Types

Source data types are preserved during extraction.

- Identifiers use UUID
- Monetary and measurement values use NUMERIC
- Date values use DATE
- Date and time values use TIMESTAMPTZ
- Text values use TEXT

NULL values are preserved and are not automatically replaced with empty values or default values.

### Schema Consistency

The source schema is checked against the expected extraction contract before processing.

If a required table or column is missing, renamed, or has an incompatible data type, the extraction is stopped for review.

No data type conversion or default value is applied without confirmation.

Changes to the source schema are documented and reviewed before the extraction contract is updated.

### Source-to-Standardized Mapping

The extracted column names remain consistent with the source fields. The extraction does not perform business transformations or standardization.

---

## 7. Extraction Acceptance and Handover Rules

The extraction result is evaluated based on the validation results.

| Validation Condition        | Acceptance Criteria                                                                                                                    | Pipeline Action                                   | Decision      |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------- | ------------- |
| All critical checks pass    | Required tables and columns exist, records are complete, IDs are valid, and extraction is complete                                     | Prepare the extraction package for the next stage | APPROVED      |
| Non-critical warnings occur | Data is extracted but warnings such as nullable dates or unusual record counts are present                                             | Record the warnings and require review            | MANUAL REVIEW |
| Critical validation fails   | Required source data is unavailable, required fields are missing, IDs are duplicated, counts do not match, or extraction is incomplete | Stop the handover and resolve the issue           | REJECTED      |

### Handover Output

The extraction package contains:

1. Extracted `branches` dataset
2. Extracted `customers` dataset
3. Extracted `orders` dataset
4. Extracted `payments` dataset
5. Extracted `expenses` dataset
6. Extraction manifest
7. Validation results
8. Pipeline run metadata
9. Acceptance decision

The package is handed over to the Transformation Stage only after the required extraction checks have been completed.

---

## 8. Common Extraction Problems and Mitigation

| Problem                                 | Possible Cause                                                   | Impact                                                        | Mitigation                                                                 |
| --------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Supabase database is unreachable        | Database or network connection problem                           | Extraction cannot start or complete                           | Check the database connection and retry the extraction                     |
| Unauthorized database access            | Incorrect credentials or missing read permissions                | Required tables cannot be read                                | Verify the authorized read-only database access                            |
| Source schema changes                   | Table or column renamed, removed, or changed                     | Extraction may fail or produce an incompatible dataset        | Compare the live schema with the extraction contract and review the change |
| Extraction is interrupted               | Connection failure or process interruption                       | Output may be incomplete                                      | Mark the run as failed and perform a new extraction                        |
| Record count mismatch                   | Records were added, changed, or missed during extraction         | Extracted dataset may not represent the complete source       | Compare source and extracted record counts and review the extraction run   |
| Duplicate or missing IDs                | Source data integrity problem                                    | Records may not be uniquely identifiable                      | Record the validation failure and stop the handover when critical          |
| Missing or unmatched references         | Invalid branch, customer, or order references                    | Relationships may not be reliable                             | Record the issue for review without modifying the extracted source data    |
| Missing dates                           | Source date fields contain NULL values                           | Some records may not be usable for later date-based reporting | Preserve the records and record the condition as a validation warning      |
| Late payments or historical corrections | Source records are updated after their original transaction date | Previous reporting periods may change                         | Use full-history extraction so updated records are included                |
| Data serialization problem              | Source values cannot be represented correctly in the output      | Extracted dataset may be incomplete or corrupted              | Validate data types and output serialization before handover               |
| Incorrect schedule time                 | Time zone or scheduling configuration is incorrect               | Extraction may run at the wrong time                          | Verify the schedule uses Asia/Manila time                                  |
| Increasing extraction volume            | Full-history extraction grows as more records are added          | Extraction may take longer over time                          | Monitor extraction duration and record counts                              |

---

## Expected Output

The completed Stage 1 extraction produces five raw datasets from the I&C Laundry PostgreSQL database:

- `branches`
- `customers`
- `orders`
- `payments`
- `expenses`

The extraction package also includes the extraction manifest, run metadata, validation results, and acceptance decision.

The extracted data preserves the source values, data types, NULL values, and original row-level structure.

The output is validated and packaged for the Transformation Stage.

Cleaning, standardization, business filtering, and reporting calculations are not performed during this extraction stage.
