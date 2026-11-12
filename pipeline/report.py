"""Short, plain-language run summaries for the terminal (docs/Load/Load.md Section 8).

Both `python -m pipeline.load` and `python -m pipeline.scheduling run` print the
same summary: one line per stage, whether the dashboard data is current, and
what the user needs to do next.
"""

import re
from datetime import date, datetime, timedelta, timezone

# Asia/Manila has no daylight saving time, so a fixed +08:00 offset is exact.
MANILA = timezone(timedelta(hours=8), 'Asia/Manila')
# Stage 2 quarantine tolerance (docs/Transform/transformation.md Section 10).
AUTO_PUBLISH_MAX_PERCENT = 5
MANUAL_REVIEW_MAX_PERCENT = 10
QUARANTINE_COMMAND = 'python -m pipeline.quarantine'
FIX_COMMAND = f'{QUARANTINE_COMMAND} --fix'
RUN_COMMAND = 'python -m pipeline.load'
RUN_ID_PATTERN = re.compile(r'Run ID: ([0-9a-f-]{36})')

LOAD_PROBLEMS = {
    'LOAD_LOCK_BUSY': 'another load was publishing at the same time',
    'STALE_BATCH': 'a newer run was already published',
    'DUPLICATE_BATCH': 'this batch was already published',
    'HISTORY_REMOVAL_HELD': 'some published days would disappear; approval is needed',
    'HANDOVER_NOT_APPROVED': 'Stage 2 did not approve the batch',
    'HANDOVER_INCOMPLETE': 'the Stage 2 handover was missing information',
    'HANDOVER_MISMATCH': 'the Stage 2 handover did not match',
    'SCHEMA_MISMATCH': 'the data did not match the Load contract',
    'INPUT_COUNT_MISMATCH': 'some rows were lost during the handover',
    'PERMISSION_DENIED': 'the database account is missing a permission',
    'STORAGE_LIMIT': 'the database is almost full',
    'FOREIGN_KEY_VIOLATION': 'a branch in the output no longer exists',
    'RECONCILIATION_FAILED': 'the published data did not match the received data',
    'SNAPSHOT_NOT_COMMITTED': 'the extraction did not finish',
}


def manila_time(value) -> str:
    """'Nov 12, 2026 09:15' in Asia/Manila."""
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    return value.astimezone(MANILA).strftime('%b %d, %Y %H:%M')


def calendar_day(value) -> str:
    if isinstance(value, str):
        value = date.fromisoformat(value[:10])
    return value.strftime('%b %d, %Y')


def plural(count, word) -> str:
    return f'{count} {word}' if count == 1 else f'{count} {word}s'


def extraction_run_id(details):
    """The extraction run ID printed at the start of a run, if any."""
    match = RUN_ID_PATTERN.search(details or '')
    return match.group(1) if match else None


def quarantine_summary(stage2):
    """(quarantined count, worst stream, worst rate, quarantined, received) from Stage 2 metadata."""
    stage2 = stage2 or {}
    count = int(stage2.get('quarantined_records') or 0)
    rates = stage2.get('quarantine_rates')
    if not rates:
        # Earlier Stage 2 versions report the counts only in their accounting.
        rates = {item['table_name']: {'quarantined': item.get('records_rejected') or 0,
                                      'received': item.get('records_read') or 0}
                 for item in stage2.get('accounting') or []
                 if item.get('table_name') in ('orders', 'payments', 'expenses')}
    worst = None
    for table, item in sorted(rates.items()):
        received = item.get('received') or 0
        if received:
            rate = 100.0 * (item.get('quarantined') or 0) / received
            if worst is None or rate > worst[1]:
                worst = (table, rate, item.get('quarantined') or 0, received)
    return count, worst


def _quarantine_text(stage2):
    count, worst = quarantine_summary(stage2)
    if not count:
        return 'nothing quarantined'
    if worst is None:
        return f'{count} quarantined'
    table, rate, _, _ = worst
    return f'{count} quarantined ({table} {rate:.1f}%, limit {AUTO_PUBLISH_MAX_PERCENT}%)'


def _over_limit_text(stage2, fallback):
    """'3 of 35 orders quarantined (8.6%), over the 5% limit' for a held or rejected batch."""
    _, worst = quarantine_summary(stage2)
    if not str((stage2 or {}).get('error_code', '')).startswith('QUARANTINE') or worst is None:
        return fallback
    table, rate, quarantined, received = worst
    limit = MANUAL_REVIEW_MAX_PERCENT if rate > MANUAL_REVIEW_MAX_PERCENT else AUTO_PUBLISH_MAX_PERCENT
    return f'{quarantined} of {received} {table} quarantined ({rate:.1f}%), over the {limit}% limit'


def stage_lines(extraction, stage2, load_entry):
    """(stage, state, detail) for each stage, in plain words."""
    lines = []
    extracted = bool(extraction) and extraction.get('acceptance_status') in (
        'APPROVED', 'APPROVED_WITH_WARNINGS')
    if not extraction:
        lines.append(('Extract', 'FAILED', 'did not finish'))
    elif not extracted:
        lines.append(('Extract', 'FAILED', extraction.get('error_message') or 'the source checks failed'))
    else:
        detail = (f"{extraction.get('records_read') or 0} records read from "
                  f"{plural(extraction.get('tables') or 0, 'table')}")
        if extraction['acceptance_status'] == 'APPROVED_WITH_WARNINGS':
            detail += ' (with warnings)'
        lines.append(('Extract', 'OK', detail))

    status = (stage2 or {}).get('status')
    approved = status in ('PASS', 'PARTIAL')
    if not extracted:
        lines.append(('Transform', 'SKIPPED', 'no data was handed over'))
    elif not stage2:
        lines.append(('Transform', 'FAILED', 'did not finish'))
    elif approved:
        lines.append(('Transform', 'OK', f"{stage2.get('records_output') or 0} daily rows, "
                                         f'{_quarantine_text(stage2)}'))
    elif status == 'WARNING':
        lines.append(('Transform', 'ON HOLD', _over_limit_text(
            stage2, stage2.get('error_message') or _quarantine_text(stage2))))
    else:
        lines.append(('Transform', 'FAILED', _over_limit_text(
            stage2, stage2.get('error_message') or 'rejected')))

    decision = (load_entry or {}).get('acceptance_status')
    if not approved:
        lines.append(('Load', 'SKIPPED', 'nothing was published'))
    elif not load_entry:
        lines.append(('Load', 'FAILED', 'did not finish'))
    elif decision == 'APPROVED':
        detail = (f"{load_entry.get('records_received') or 0} rows published: "
                  f"{load_entry.get('records_inserted') or 0} new, "
                  f"{load_entry.get('records_updated') or 0} changed, "
                  f"{load_entry.get('records_unchanged') or 0} unchanged")
        if load_entry.get('records_deleted'):
            detail += f", {load_entry['records_deleted']} removed"
        lines.append(('Load', 'OK', detail))
    else:
        code = load_entry.get('error_code')
        detail = LOAD_PROBLEMS.get(code) or load_entry.get('error_message') or 'not published'
        lines.append(('Load', 'ON HOLD' if decision == 'MANUAL REVIEW' else 'FAILED', detail))
    return lines


def next_steps(extraction, stage2, load_entry, *, details='', notes=()):
    """What the user should do, as short instructions with the command to run."""
    steps = []
    if 'queued_logs' in notes:
        return ['Check your internet connection and the password in .env, then deliver the '
                'queued logs  ->  python -m pipeline.load --flush-logs']
    if not extraction:
        if 'Could not connect' in (details or ''):
            steps.append('Check your internet connection and the password in .env, then run again.')
        elif 'Another extraction' in (details or ''):
            steps.append('Another run is in progress. Wait a minute, then run again.')
        else:
            steps.append('Read the details log to see what stopped the run.')
    elif extraction.get('acceptance_status') not in ('APPROVED', 'APPROVED_WITH_WARNINGS'):
        if extraction.get('error_code') == 'EXTRACTION_ALREADY_RUNNING':
            steps.append('Another run is in progress. Wait a minute, then run again.')
        else:
            steps.append('Fix the source problem named above, then run again.')

    count, _ = quarantine_summary(stage2)
    status = (stage2 or {}).get('status')
    if status in ('WARNING', 'FAIL') and str((stage2 or {}).get('error_code', '')).startswith('QUARANTINE'):
        steps.append(f'Fix the quarantined records  ->  {FIX_COMMAND}')
        steps.append(f'Run the pipeline again  ->  {RUN_COMMAND}')
    elif status == 'FAIL' and extraction:
        steps.append('Read the details log to see which Stage 2 check failed.')
    elif count:
        steps.append(f"{plural(count, 'quarantined record')} {'needs' if count == 1 else 'need'} "
                     f'fixing  ->  {FIX_COMMAND}')

    code = (load_entry or {}).get('error_code')
    if load_entry and load_entry.get('acceptance_status') != 'APPROVED':
        if code in ('LOAD_LOCK_BUSY',):
            steps.append('Another run is publishing. Wait a minute, then run again.')
        elif code == 'HISTORY_REMOVAL_HELD':
            steps.append('Check the held days in etl_ops.load_rejections '
                         f"(load_run_id {load_entry.get('load_run_id')}); approve removals in "
                         'etl_ops.load_review_approvals, then run again.')
        elif code not in ('STALE_BATCH', 'DUPLICATE_BATCH'):
            steps.append('Read the details log to see which Load check failed.')
    if 'transformation_logs' in notes or 'load_logs' in notes:
        steps.append('Some logs are not saved yet  ->  python -m pipeline.load --flush-logs')
    return steps


def render(started, extraction, stage2, load_entry, *, last_published=None, details_hint=None,
           details='', database_message=None, notes=()):
    """The short summary printed at the end of a run."""
    out = [f'I&C Laundry ETL - {manila_time(started)} (Asia/Manila)', '']
    if 'queued_logs' in notes:
        out.append('The run did not start: logs from an earlier run could not be delivered.')
    else:
        for number, (stage, state, detail) in enumerate(stage_lines(extraction, stage2, load_entry), 1):
            out.append(f'  {number}. {stage:<10} {state:<8} {detail}')
    out.append('')

    if load_entry and load_entry.get('acceptance_status') == 'APPROVED' and load_entry.get('reporting_cutoff'):
        through = date.fromisoformat(str(load_entry['reporting_cutoff'])[:10]) - timedelta(days=1)
        out.append(f'Dashboard data is up to date through {calendar_day(through)}.')
    elif last_published:
        out.append(f'Nothing was published. The dashboard still shows the run from '
                   f'{manila_time(last_published)}.')
    else:
        out.append('Nothing was published, and no data has been published yet.')

    if database_message and not any(state in ('FAILED', 'ON HOLD')
                                    for _, state, _ in stage_lines(extraction, stage2, load_entry)):
        out.append(f'Database message: {database_message}')

    steps = next_steps(extraction, stage2, load_entry, details=details, notes=notes)
    out.append('')
    if steps:
        out.append('To do:')
        out.extend(f'  - {step}' for step in steps)
    else:
        out.append('Nothing to do.')
    if details_hint:
        out.extend(['', f'Details: {details_hint}'])
    return '\n'.join(out)


def exit_code(extraction, stage2, load_entry) -> int:
    """0 published, 2 held for review, 1 anything else."""
    if load_entry:
        return {'APPROVED': 0, 'MANUAL REVIEW': 2}.get(load_entry.get('acceptance_status'), 1)
    return 2 if (stage2 or {}).get('acceptance_status') == 'MANUAL REVIEW' else 1


# Read-only lookups used to build the summary.

def fetch_extraction(cursor, run_id):
    cursor.execute('''
        SELECT count(*), sum(records_read), max(acceptance_status),
               max(error_code), max(error_message)
        FROM etl_ops.extraction_logs WHERE pipeline_run_id = %s
    ''', (run_id,))
    tables, read, acceptance, code, message = cursor.fetchone()
    if not tables:
        return None
    return {'tables': tables, 'records_read': read, 'acceptance_status': acceptance,
            'error_code': code, 'error_message': message}


def fetch_last_publication(cursor):
    cursor.execute('SELECT max(published_at) FROM etl_ops.load_publications')
    return cursor.fetchone()[0]


def fetch_scheduled_run(cursor, since):
    """Extraction, Stage 2, and Load results of the scheduled run started at `since`."""
    cursor.execute('''
        SELECT pipeline_run_id FROM etl_ops.extraction_logs
        WHERE extraction_started_at >= %s ORDER BY extraction_started_at DESC LIMIT 1
    ''', (since,))
    row = cursor.fetchone()
    extraction = fetch_extraction(cursor, row[0]) if row else None
    cursor.execute('''
        SELECT metadata FROM etl_ops.transformation_logs
        WHERE ended_at >= %s ORDER BY ended_at DESC LIMIT 1
    ''', (since,))
    row = cursor.fetchone()
    stage2 = row[0] if row else None
    cursor.execute('''
        SELECT load_run_id, acceptance_status, load_status, error_code, error_message,
               records_received, records_inserted, records_updated, records_unchanged,
               records_deleted, reporting_cutoff
        FROM etl_ops.load_logs WHERE load_started_at >= %s
        ORDER BY load_started_at DESC LIMIT 1
    ''', (since,))
    columns = [column.name for column in cursor.description]
    row = cursor.fetchone()
    load_entry = dict(zip(columns, row)) if row else None
    return extraction, stage2, load_entry, fetch_last_publication(cursor)
