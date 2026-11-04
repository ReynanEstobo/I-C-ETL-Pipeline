"""Exercise installed Stage 2 rules using session-local synthetic datasets."""

from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from pipeline.connection import connect_source, SourceConnectionError
from pipeline.extraction import SOURCE_COLUMNS


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    branch, other_branch, customer, order = [uuid4() for _ in range(4)]
    stamp = datetime(2026, 10, 12, 2, tzinfo=timezone.utc)
    fixtures = {
        'branches': [(branch, 'Test branch', None, None),
                     (other_branch, 'Other branch', None, None)],
        'customers': [(customer,)],
        'orders': [(order, 'TEST-001', branch, customer, stamp, 'released', 8, 200)],
        'payments': [(uuid4(), order, branch, 100, stamp, 'test'),
                     (uuid4(), order, branch, 100, stamp, 'test')],
        'expenses': [(uuid4(), branch, 50, stamp.date(), None),
                     (uuid4(), branch, 25, stamp.date(), stamp)],
    }
    cases = [
        ('Valid totals, multiple payments, and deleted expense', None, 'PASS', None),
        ('Negative weight', 'UPDATE pg_temp.stage_orders SET weight_kg=-1', 'PARTIAL', 'C-04'),
        ('Unknown status', "UPDATE pg_temp.stage_orders SET status='unknown'", 'PARTIAL', 'S-03'),
        ('Zero payment', 'UPDATE pg_temp.stage_payments SET amount=0', 'PARTIAL', 'C-09'),
        ('Negative payment', 'UPDATE pg_temp.stage_payments SET amount=-1', 'PARTIAL', 'C-09'),
        ('Missing numeric', 'UPDATE pg_temp.stage_orders SET weight_kg=NULL', 'PARTIAL', 'C-03'),
        ('Non-finite numeric', "UPDATE pg_temp.stage_orders SET weight_kg='NaN'::numeric", 'PARTIAL', 'C-03'),
        ('Negative expense', 'UPDATE pg_temp.stage_expenses SET amount=-1', 'PARTIAL', 'C-08'),
        ('Cross-branch payment', sql.SQL('UPDATE pg_temp.stage_payments SET branch_id={}').format(sql.Literal(other_branch)), 'PARTIAL', 'C-07'),
        ('Cancelled order preserves payments', "UPDATE pg_temp.stage_orders SET status='cancelled'", 'PASS', None),
        ('Status whitespace and case', "UPDATE pg_temp.stage_orders SET status=' Released '", 'PASS', None),
        ('Current-day order deferred', "UPDATE pg_temp.stage_orders SET created_at='2026-10-23 02:00:00+00'", 'PASS', None),
    ]
    with connect_source() as connection:
        connection.read_only = False
        for table, columns in SOURCE_COLUMNS.items():
            definitions = sql.SQL(', ').join(
                sql.SQL('{} {}').format(sql.Identifier(column), sql.SQL(kind))
                for column, kind in columns.items())
            connection.execute(sql.SQL('CREATE TEMP TABLE {} ({}) ON COMMIT DELETE ROWS').format(
                sql.Identifier('stage_' + table), definitions))
        connection.execute('SELECT etl_ops.prepare_transformation()')
        connection.commit()
        connection.read_only = True
        connection.isolation_level = psycopg.IsolationLevel.REPEATABLE_READ
        for label, mutation, expected, rule in cases:
            with connection.transaction():
                for table, rows in fixtures.items():
                    for row in rows:
                        connection.execute(sql.SQL('INSERT INTO {} VALUES ({})').format(
                            sql.Identifier('pg_temp', 'stage_' + table),
                            sql.SQL(', ').join(sql.Placeholder() for _ in row)), row)
                if mutation is not None:
                    connection.execute(mutation)
                payload = {'pipeline_run_id': str(uuid4()), 'acceptance_status': 'APPROVED',
                           'extraction_started_at': '2026-10-23T00:00:00+00:00'}
                def consume():
                    return connection.execute('SELECT etl_ops.consume_transformation(%s)',
                                              (Jsonb(payload),)).fetchone()[0]['transformation_run']
                result = consume()
                require(result['status'] == expected, f'{label}: expected {expected}, got {result}')
                decision = {'PASS': 'APPROVED', 'PARTIAL': 'APPROVED_WITH_QUARANTINE',
                            'WARNING': 'MANUAL REVIEW', 'FAIL': 'REJECTED'}[expected]
                require(result['acceptance_status'] == decision, f'{label}: incorrect acceptance')
                if rule:
                    require(any(issue['rule_id'] == rule for issue in result['issues']), f'{label}: missing {rule}')
                rows = connection.execute('SELECT * FROM pg_temp.transformed_daily_branch_summary ORDER BY report_date, branch_id').fetchall()
                if expected in ('WARNING', 'FAIL'):
                    require(not rows, f'{label}: blocked batch retained output')
                else:
                    excluded_order = label.startswith(('Cancelled', 'Current-day'))
                    quarantined_order = expected == 'PARTIAL' and label in {
                        'Negative weight', 'Unknown status', 'Missing numeric', 'Non-finite numeric'}
                    quarantined_payment = expected == 'PARTIAL' and label in {
                        'Zero payment', 'Negative payment', 'Cross-branch payment'}
                    quarantined_expense = expected == 'PARTIAL' and label == 'Negative expense'
                    expected_row = (date(2026, 10, 12), branch,
                                    0 if excluded_order or quarantined_order else 1,
                                    0 if excluded_order or quarantined_order else 1,
                                    0 if excluded_order or quarantined_order else 1,
                                    Decimal(0 if excluded_order or quarantined_order else 8),
                                    Decimal(0 if excluded_order or quarantined_order else 200),
                                    0 if quarantined_payment else 2,
                                    Decimal(0 if quarantined_payment else 200),
                                    Decimal(0 if quarantined_expense else 50))
                    require(rows == [expected_row], f'{label}: incorrect aggregate totals: {rows}')
                    consume()
                    rerun = connection.execute('SELECT * FROM pg_temp.transformed_daily_branch_summary ORDER BY report_date, branch_id').fetchall()
                    require(rerun == rows, f'{label}: rerun changed output')
                print(f'PASS: {label} -> {decision}', flush=True)
    print(f'All {len(cases)} Stage 2 tests passed. Temporary datasets removed; no source records or permanent logs written.')


if __name__ == '__main__':
    try:
        main()
    except (AssertionError, psycopg.Error, SourceConnectionError) as error:
        print(f'Stage 2 test failed: {error}')
        raise SystemExit(1)
