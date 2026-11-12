"""Records quarantined by the latest Stage 2 run, and how to fix them.

    python -m pipeline.quarantine            show the fix list
    python -m pipeline.quarantine --export   also save it as CSV in quarantine/
    python -m pipeline.quarantine --fix      fix the records one by one in the terminal

Quarantined records are fixed in their source tables. --fix changes only the
fields a rule flagged, checks each new value against the same rule, asks before
saving, and records every change in etl_ops.quarantine_fixes. The next pipeline
run checks the records again, and fixed records leave the list
(docs/Transform/transformation.md Section 9).
"""

import csv
import getpass
import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg import sql

from pipeline import report
from pipeline.connection import SourceConnectionError, connect_source

USAGE = 'Usage: python -m pipeline.quarantine [--export | --fix]'
EXPORT_DIRECTORY = Path(__file__).resolve().parents[1] / 'quarantine'
COLUMNS = (
    'table_name', 'record_label', 'record_id', 'branch', 'rules', 'problem', 'current_values',
    'record_missing', 'how_to_fix', 'edit_in', 'found_by_run_at', 'first_seen', 'times_quarantined',
)
# The fields each Stage 2 rule is about, so the list shows only what needs attention.
RULE_FIELDS = {
    'C-02': ('status',),
    'C-03': ('weight_kg', 'total_price', 'amount'),
    'C-04': ('weight_kg',),
    'C-05': ('order_number',),
    'C-06': ('branch_id', 'customer_id', 'created_at', 'order_id', 'paid_at', 'expense_date'),
    'C-07': ('branch_id', 'order_branch_id'),
    'C-08': ('total_price', 'amount'),
    'C-09': ('amount',),
    'S-03': ('status',),
}
STATUSES = ('received', 'on_process', 'ready', 'released', 'cancelled')
# The source fields --fix may change, with their database types. Anything else,
# such as removing a duplicate (C-05), is done in Supabase.
EDITABLE = {
    ('orders', 'status'): 'text',
    ('orders', 'weight_kg'): 'numeric',
    ('orders', 'total_price'): 'numeric',
    ('orders', 'branch_id'): 'uuid',
    ('orders', 'customer_id'): 'uuid',
    ('orders', 'created_at'): 'timestamptz',
    ('payments', 'amount'): 'numeric',
    ('payments', 'branch_id'): 'uuid',
    ('payments', 'paid_at'): 'timestamptz',
    ('expenses', 'amount'): 'numeric',
    ('expenses', 'branch_id'): 'uuid',
    ('expenses', 'expense_date'): 'date',
}
# Current values of the editable fields, read from the source when fixing.
SOURCE_QUERIES = {
    'orders': '''SELECT status, weight_kg, total_price, branch_id, customer_id, created_at
                 FROM public.orders WHERE id = %s::uuid''',
    'payments': '''SELECT p.amount, p.branch_id, p.paid_at, o.branch_id AS order_branch_id
                   FROM public.payments p LEFT JOIN public.orders o ON o.id = p.order_id
                   WHERE p.id = %s::uuid''',
    'expenses': '''SELECT amount, branch_id, expense_date
                   FROM public.expenses WHERE id = %s::uuid''',
}
TABLES_WITH_UPDATED_AT = ('orders', 'payments')


def fetch(cursor) -> list:
    cursor.execute(f'''
        SELECT {', '.join(COLUMNS)} FROM etl_ops.quarantine_review
        ORDER BY table_name, record_label, record_id
    ''')
    return [dict(zip(COLUMNS, row)) for row in cursor.fetchall()]


# Rule checks shared by the list and --fix.
def to_number(value):
    """A Decimal for a number or numeric text, else None."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError):
        return None


def value_problem(table, field, value, order_branch=None):
    """Why a value breaks the Stage 2 rules for this field, or None when it passes."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return f'{field} is empty'
    if field == 'status':
        if any(ord(character) < 32 for character in value) or value.strip().lower() not in STATUSES:
            return f'status must be one of: {", ".join(STATUSES)}'
    elif EDITABLE[(table, field)] == 'numeric':
        number = to_number(value)
        if number is None or not number.is_finite():
            return f'{field} must be a number'
        if table == 'payments' and number <= 0:
            return 'amount must be more than 0'
        if number < 0:
            return f'{field} must be 0 or more'
    elif field == 'branch_id' and order_branch is not None and str(value) != str(order_branch):
        return "branch_id must be the branch of the payment's order"
    return None


def rule_list(row) -> list:
    return [rule for rule in (row['rules'] or '').split(', ') if rule]


def flagged_fields(row) -> list:
    """The fields --fix can change for the rules this record broke."""
    fields = []
    for rule in rule_list(row):
        fields.extend(name for name in RULE_FIELDS.get(rule, ())
                      if (row['table_name'], name) in EDITABLE)
    return list(dict.fromkeys(fields))


def manual_rules(row) -> list:
    """Rules that --fix cannot address, such as a duplicate record (C-05)."""
    return [rule for rule in rule_list(row)
            if not any((row['table_name'], name) in EDITABLE for name in RULE_FIELDS.get(rule, ()))]


def fields_to_fix(row, values) -> list:
    """Flagged fields whose current value still breaks a rule."""
    order_branch = values.get('order_branch_id') if row['table_name'] == 'payments' else None
    return [name for name in flagged_fields(row)
            if value_problem(row['table_name'], name, values.get(name), order_branch)]


def looks_fixed(row) -> bool:
    values = row['current_values'] or {}
    return (not row['record_missing'] and bool(flagged_fields(row)) and not manual_rules(row)
            and not fields_to_fix(row, values))


# The fix list.
def current_focus(row) -> str:
    """The current values of the fields the rule is about, e.g. 'weight_kg = -2'."""
    if row['record_missing']:
        return 'the record no longer exists; it leaves this list after the next run'
    values = row['current_values'] or {}
    fields = []
    for rule in rule_list(row):
        fields.extend(name for name in RULE_FIELDS.get(rule, ()) if name in values)
    fields = list(dict.fromkeys(fields)) or list(values)
    text = ', '.join(f'{name} = {"empty" if values[name] is None else values[name]}'
                     for name in fields)
    return f'{text}  (looks fixed; the next run will confirm)' if looks_fixed(row) else text


def render(rows) -> str:
    if not rows:
        return 'No quarantined records. Nothing to fix.'
    found = report.manila_time(rows[0]['found_by_run_at'])
    out = [f"{report.plural(len(rows), 'record')} {'needs' if len(rows) == 1 else 'need'} "
           f'fixing (found by the run on {found}).', '']
    for number, row in enumerate(rows, 1):
        label = row['record_label'] or row['record_id']
        branch = f"  ({row['branch']})" if row['branch'] else ''
        out += [
            f"  {number}. {row['table_name']}  {label}{branch}",
            f"     Problem: {row['problem']}",
            f'     Now:     {current_focus(row)}',
            f"     Fix:     {row['how_to_fix']}",
            f"     Where:   Supabase > Table Editor > {row['edit_in']} > row id {row['record_id']}",
            f"     Quarantined in {report.plural(row['times_quarantined'], 'run')} "
            f"since {report.calendar_day(row['first_seen'])}.",
            '',
        ]
    out += [f'Fix them here in the terminal: {report.FIX_COMMAND}',
            f'After fixing, run the pipeline again: {report.RUN_COMMAND}',
            'Fixed records leave this list after the next run.']
    return '\n'.join(out)


def export(rows) -> Path:
    """Save the fix list as CSV in quarantine/ and return the file path."""
    EXPORT_DIRECTORY.mkdir(exist_ok=True)
    stamp = datetime.now(timezone.utc).astimezone(report.MANILA).strftime('%Y%m%d-%H%M%S')
    path = EXPORT_DIRECTORY / f'quarantine-{stamp}.csv'
    with path.open('w', newline='', encoding='utf-8') as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({**row, 'current_values': json.dumps(row['current_values'], default=str)})
    return path


# --fix: one record at a time, one transaction per record.
class Stop(Exception):
    """The user typed q, or the input ended."""


class RecordChanged(Exception):
    """The record was changed by someone else after it was shown."""


def ask(prompt, read) -> str:
    try:
        answer = read(prompt).strip()
    except (EOFError, KeyboardInterrupt):
        print()
        raise Stop from None
    if answer.lower() in ('q', 'quit'):
        raise Stop
    return answer


def shown(name, value, branches) -> str:
    if value is None:
        return 'empty'
    if name in ('branch_id', 'order_branch_id'):
        return branches.get(str(value), str(value))
    if isinstance(value, datetime):
        return value.astimezone(report.MANILA).strftime('%Y-%m-%d %H:%M (Manila)')
    return str(value)


def as_text(value):
    """How a value is written in the fix log."""
    if value is None:
        return None
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value)


def read_source(cursor, row):
    cursor.execute(SOURCE_QUERIES[row['table_name']], (row['record_id'],))
    found = cursor.fetchone()
    return None if found is None else dict(zip((column.name for column in cursor.description), found))


def parse(cursor, table, name, answer, branches):
    """Turn what was typed into a database value: (value, problem)."""
    kind = EDITABLE[(table, name)]
    if name == 'status':
        return answer.lower(), None
    if kind == 'numeric':
        number = to_number(answer)
        if number is None or not number.is_finite():
            return None, f'{name} must be a number'
        return number, None
    if name == 'branch_id':
        choices = list(branches)
        if answer.isdigit() and 1 <= int(answer) <= len(choices):
            return UUID(choices[int(answer) - 1]), None
        return None, f'choose a number from 1 to {len(choices)}'
    if name == 'customer_id':
        try:
            customer = UUID(answer)
        except ValueError:
            return None, 'that is not a customer id'
        cursor.execute('SELECT name FROM public.customers WHERE id = %s AND deleted_at IS NULL',
                       (customer,))
        found = cursor.fetchone()
        if found is None:
            return None, 'no active customer has that id'
        print(f'      Customer: {found[0]}')
        return customer, None
    if kind == 'timestamptz':
        try:
            return datetime.strptime(answer, '%Y-%m-%d %H:%M').replace(tzinfo=report.MANILA), None
        except ValueError:
            return None, 'use the format YYYY-MM-DD HH:MM'
    try:
        return datetime.strptime(answer, '%Y-%m-%d').date(), None
    except ValueError:
        return None, 'use the format YYYY-MM-DD'


def prompt_value(cursor, table, name, values, branches, read):
    """Ask for a new value until it passes the rule; None when skipped."""
    kind = EDITABLE[(table, name)]
    order_branch = values.get('order_branch_id') if table == 'payments' else None
    print(f'  {name} is {shown(name, values.get(name), branches)}')
    if name == 'branch_id':
        for number, (branch_id, branch_name) in enumerate(branches.items(), 1):
            mark = "  (the order's branch)" if str(order_branch) == branch_id else ''
            print(f'      {number}. {branch_name}{mark}')
        prompt = f'  Choose a branch (1-{len(branches)}): '
    elif name == 'status':
        prompt = f'  New status ({", ".join(STATUSES)}): '
    elif name == 'customer_id':
        prompt = '  New customer_id (the id from public.customers): '
    elif kind == 'timestamptz':
        prompt = f'  New {name} (YYYY-MM-DD HH:MM, Manila time): '
    elif kind == 'date':
        prompt = f'  New {name} (YYYY-MM-DD): '
    else:
        prompt = f'  New {name}: '
    while True:
        answer = ask(prompt, read)
        if not answer:
            return None
        value, problem = parse(cursor, table, name, answer, branches)
        if problem is None:
            problem = value_problem(table, name, value, order_branch)
        if problem is None and value == values.get(name):
            problem = 'that is the current value'
        if problem is None:
            return value
        print(f'    {problem[0].upper()}{problem[1:]}. Try again, or press Enter to skip.')


def make_writable(cursor):
    cursor.execute('SHOW transaction_read_only')
    if cursor.fetchone()[0] == 'on':
        cursor.execute('SET TRANSACTION READ WRITE')


def save_changes(connection, row, changes, fixed_by):
    """Apply the changes and log them in one transaction.

    Each change is (field, value shown, new value). The update only succeeds
    while the field still holds the value shown; otherwise nothing is saved.
    """
    table = row['table_name']
    touch = sql.SQL(', updated_at = now()' if table in TABLES_WITH_UPDATED_AT else '')
    with connection.transaction():
        with connection.cursor() as cursor:
            make_writable(cursor)
            for name, old, new in changes:
                cast = sql.SQL(EDITABLE[(table, name)])
                cursor.execute(sql.SQL(
                    'UPDATE public.{table} SET {field} = %s::{cast}{touch} '
                    'WHERE id = %s::uuid AND {field} IS NOT DISTINCT FROM %s::{cast}'
                ).format(table=sql.Identifier(table), field=sql.Identifier(name), cast=cast, touch=touch),
                    (new, row['record_id'], old))
                if cursor.rowcount != 1:
                    raise RecordChanged
                cursor.execute('''
                    INSERT INTO etl_ops.quarantine_fixes
                        (fixed_by, table_name, record_id, rules, field_name, old_value, new_value,
                         found_by_run_at)
                    VALUES (%s, %s, %s::uuid, %s, %s, %s, %s, %s)
                ''', (fixed_by, table, row['record_id'], row['rules'], name, as_text(old),
                      as_text(new), row['found_by_run_at']))


def fix_record(connection, row, branches, fixed_by, read) -> str:
    """Walk through one record. Returns fixed, skipped, already, manual, or missing."""
    label = row['record_label'] or row['record_id']
    branch = f" ({row['branch']})" if row['branch'] else ''
    print(f"{row['table_name']} {label}{branch}")
    print(f"  Problem: {row['problem']}")
    print(f"  Fix:     {row['how_to_fix']}")
    where = f"  Where:   Supabase > Table Editor > {row['edit_in']} > row id {row['record_id']}"
    with connection.cursor() as cursor:
        values = read_source(cursor, row)
        if values is None:
            print('  The record no longer exists; it leaves the list after the next run.')
            return 'missing'
        fields = fields_to_fix(row, values)
        manual = manual_rules(row)
        if not fields:
            if manual:
                print(f"  {', '.join(manual)} can't be fixed here. Fix it in Supabase:")
                print(where)
                return 'manual'
            now = ', '.join(f'{name} = {shown(name, values.get(name), branches)}'
                            for name in flagged_fields(row))
            print(f'  Already fixed ({now}). The next run will confirm.')
            return 'already'
        print()
        changes = []
        for name in fields:
            new = prompt_value(cursor, row['table_name'], name, values, branches, read)
            if new is not None:
                changes.append((name, values.get(name), new))
    if manual:
        print(f"  {', '.join(manual)} also needs fixing in Supabase:")
        print(where)
    if not changes:
        print('  Skipped.')
        return 'skipped'
    summary = [f'{name}: {shown(name, old, branches)} -> {shown(name, new, branches)}'
               for name, old, new in changes]
    if len(summary) == 1:
        confirm = ask(f'  Save {summary[0]}? [y/N]: ', read)
    else:
        for line in summary:
            print(f'    {line}')
        confirm = ask(f'  Save these {len(summary)} changes? [y/N]: ', read)
    if confirm.lower() not in ('y', 'yes'):
        print('  Not saved.')
        return 'skipped'
    try:
        save_changes(connection, row, changes, fixed_by)
    except RecordChanged:
        print('  Not saved: someone changed this record after it was shown. Run --fix again.')
        return 'skipped'
    except psycopg.Error as error:
        if connection.broken:
            raise
        print(f'  Not saved (SQLSTATE: {error.sqlstate or "unavailable"}). Nothing was changed.')
        return 'skipped'
    print('  Saved.')
    return 'fixed'


def fix(connection, rows, read=input) -> int:
    if not rows:
        print('No quarantined records. Nothing to fix.')
        return 0
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass('etl_ops.quarantine_fixes')")
        if cursor.fetchone()[0] is None:
            print('The fix log is not set up yet. Run the quarantine fix SQL in Supabase first.')
            return 1
        cursor.execute('SELECT id::text, branch_name FROM public.branches ORDER BY branch_name')
        branches = dict(cursor.fetchall())
    try:
        fixed_by = getpass.getuser()
    except Exception:
        fixed_by = 'unknown'

    print(f"{report.plural(len(rows), 'record')} {'needs' if len(rows) == 1 else 'need'} fixing. "
          'Press Enter to skip, or type q to stop.')
    outcomes = []
    try:
        for number, row in enumerate(rows, 1):
            print(f'\nRecord {number} of {len(rows)}: ', end='')
            outcomes.append(fix_record(connection, row, branches, fixed_by, read))
    except Stop:
        print('Stopped.')

    count = outcomes.count
    parts = [f"{count('fixed')} fixed"]
    parts += [f'{count(name)} {text}' for name, text in (
        ('already', 'already fixed'), ('skipped', 'skipped'),
        ('manual', 'to fix in Supabase'), ('missing', 'no longer exist')) if count(name)]
    if len(outcomes) < len(rows):
        parts.append(f'{len(rows) - len(outcomes)} not reached')
    print(f"\nDone: {', '.join(parts)}.")
    if count('fixed') or count('already'):
        print(f'Run the pipeline to publish the fixed records: {report.RUN_COMMAND}')
    return 0


def main(argv=None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if args not in ([], ['--export'], ['--fix']):
        print(USAGE)
        return 1
    try:
        with connect_source() as connection:
            if args == ['--fix']:
                # Each saved record is its own read-write transaction.
                connection.read_only = False
                connection.autocommit = True
            with connection.cursor() as cursor:
                rows = fetch(cursor)
            if args == ['--fix']:
                return fix(connection, rows)
    except psycopg.errors.UndefinedTable:
        print('The quarantine fix list is not set up yet. Run the quarantine review SQL in Supabase first.')
        return 1
    except SourceConnectionError as error:
        print(error)
        return 1
    except psycopg.Error as error:
        print(f'Could not reach the fix list (SQLSTATE: {error.sqlstate or "unavailable"}).')
        return 1
    print(render(rows))
    if args == ['--export'] and rows:
        path = export(rows)
        print(f'\nSaved: {path.relative_to(EXPORT_DIRECTORY.parent).as_posix()}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
