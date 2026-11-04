SQL presentation datasets
=========================

Use Supabase SQL Editor as the ETL owner, in a dedicated editor session.
These scripts use the database's installed Stage 2 functions. They require the
existing `etl_ops` extraction log tables and functions.

1. Run all of `setup.sql` once. It creates `etl_demo`, source tables containing
   the columns your extractor reads, and a separate extraction procedure generated
   from `pipeline/scheduling.py`. It deliberately fails if `etl_demo` already exists.
2. Run all of `seed_scenarios.sql` once. It installs the scenario selector and seeds
   the valid dataset.
3. Run only `CALL etl_demo.run_scheduled_extraction();` from `run.sql`.
   Do not combine it with other statements or wrap it in a transaction: the
   extraction procedure commits internally.
4. Run `results.sql` to inspect the latest demo extraction and linked transformation.
5. To demonstrate another case, run one selector below, then repeat steps 3–4.
   Each selector restores the valid baseline before making its deliberate change.

| SQL selector | Extraction expected | Transformation expected |
| --- | --- | --- |
| `SELECT etl_demo.seed('valid');` | PASS / APPROVED | PASS / APPROVED; 1 output group |
| `SELECT etl_demo.seed('missing_branch');` | WARNING / MANUAL REVIEW | Not called |
| `SELECT etl_demo.seed('duplicate_id');` | FAIL / REJECTED | Not called |
| `SELECT etl_demo.seed('missing_date');` | FAIL / REJECTED | Not called |
| `SELECT etl_demo.seed('negative_weight');` | PASS / APPROVED | FAIL / REJECTED; C-04 |
| `SELECT etl_demo.seed('unknown_status');` | PASS / APPROVED | WARNING / MANUAL REVIEW; S-03 |
| `SELECT etl_demo.seed('zero_payment');` | PASS / APPROVED | FAIL / REJECTED; C-09 |

For rejected cases, the CALL intentionally raises an error after saving logs.
Run `results.sql` separately afterward. Warnings can return without an SQL error;
inspect acceptance status rather than treating a successful CALL as approval.
NULL transformation columns mean extraction blocked the handover for that run.

View the actual synthetic records with:

```sql
SELECT * FROM etl_demo.orders;
SELECT * FROM etl_demo.payments;
SELECT * FROM etl_demo.expenses;
```

The valid dataset has one released 8 kg / PHP 200 order, two PHP 100 payments,
and PHP 50 active expense plus PHP 25 soft-deleted expense. Its expected summary
is one order, one released order, one customer, 8 kg, PHP 200 order value, two
payments, PHP 200 cash, and PHP 50 expense for January 1, 2026. This assumes the
run occurs after that date. The scheduled procedure clears temporary output at
commit; results.sql displays metadata, not persisted summary values. Use the
existing `python -u -m pipeline.transformation_test` to assert detailed totals.

Public source records and Cron configuration are unchanged. Demo runs DO write
to `etl_ops` logs, identified by extraction table names starting `etl_demo.`;
they also invoke the normal pending-log recovery functions. The demo uses the
same overlap lock as normal extraction, so avoid running it during the daily job.
The setup's only procedure customization is clearing its temporary tables before
each call, allowing repeated demonstrations in the same dedicated session.

These are extraction-shaped fixtures, not a full clone of your app schema.
Constraints are intentionally absent in the isolated source tables so invalid
records reach ETL validation. Your public tables would reject some of them at
INSERT time, for example zero payments and duplicate primary keys.

Normal `python -m pipeline.scheduling run` still reads public data. To use these
fixtures, invoke the `etl_demo` procedure above. No permanent reporting table is
loaded. SQL files were generated and reviewed locally; execute the scenarios to
verify compatibility with your currently installed database functions.
