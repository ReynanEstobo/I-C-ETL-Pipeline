"""Stage 2 consumer: SQL transformations inside the approved extraction snapshot."""

from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from uuid import uuid4
import json
import os
import sys
import tempfile

import psycopg
from psycopg.types.json import Jsonb

from pipeline import extraction
from pipeline.connection import connect_source, SourceConnectionError

VERSION = '1.0.0'


def prepare(connection):
    connection.execute('SELECT etl_ops.prepare_transformation()')


class TransformationConsumer:
    """No source rows leave PostgreSQL; only diagnostics and counts are returned."""

    prepare = staticmethod(prepare)

    def __init__(self, loader=None):
        self.loader = loader
        self.result = None
        self.parent = None

    def acknowledge(self, metadata):
        if metadata.get('acceptance_status') != 'APPROVED':
            raise ValueError('Approved extraction required')
        self.parent = str(metadata['pipeline_run_id'])
        return metadata

    def consume(self, connection, payload):
        if str(payload['pipeline_run_id']) != self.parent or payload['acceptance_status'] != 'APPROVED':
            raise ValueError('Handover identity mismatch')
        if connection.execute('SHOW transaction_read_only').fetchone()[0] != 'on':
            raise ValueError('Read-only snapshot required')
        if connection.execute('SHOW transaction_isolation').fetchone()[0] != 'repeatable read':
            raise ValueError('Repeatable-read snapshot required')
        started = datetime.now(timezone.utc)
        timer = perf_counter()
        self.result = {
            'run_id': str(uuid4()), 'parent_run_id': self.parent,
            'started_at': started.isoformat(), 'rule_version': VERSION,
            'schema_version': VERSION, 'code_version': VERSION,
            'status': 'FAIL', 'acceptance_status': 'REJECTED',
            'handover': 'NOT_ATTEMPTED', 'log_delivery_status': 'PENDING',
        }
        try:
            # The block rolls back partial transformation writes on SQL failure.
            with connection.transaction():
                database_payload = json.loads(json.dumps(payload, default=str))
                response = connection.execute(
                    'SELECT etl_ops.consume_transformation(%s)',
                    (Jsonb(database_payload),),
                ).fetchone()[0]
                self.result = response['transformation_run']
                status = self.result['status']
                if status == 'PASS':
                    self.result['handover'] = 'NOT_CONFIGURED'
                    if self.loader is not None:
                        offered = dict(self.result, output='pg_temp.transformed_daily_branch_summary')
                        ack = self.loader.acknowledge(offered)
                        if not isinstance(ack, dict) or ack.get('run_id') != offered['run_id'] or ack.get('acceptance_status') != 'APPROVED':
                            raise ValueError('Load acknowledgment mismatch')
                        self.loader.consume(connection, offered)
                        if connection.info.transaction_status != psycopg.pq.TransactionStatus.INTRANS:
                            raise ValueError('Load ended the active transaction')
                        self.result['handover'] = 'ACKNOWLEDGED'
                else:
                    connection.execute('DELETE FROM pg_temp.transformed_daily_branch_summary')
        except Exception:
            self.result.update(status='FAIL', acceptance_status='REJECTED',
                               error_code='TRANSFORMATION_FAILED', handover='FAILED')
            raise
        finally:
            self.result['ended_at'] = datetime.now(timezone.utc).isoformat()
            self.result['duration_ms'] = int((perf_counter() - timer) * 1000)
            print(json.dumps(self.result, indent=2, default=str))


def pending_directory():
    # Reuse the existing restricted directory, with a separate file suffix so
    # extraction replay cannot interpret transformation metadata as its own.
    return extraction.private_pending_directory()


def flush_logs():
    for path in pending_directory().glob('*.transformation'):
        entry = json.loads(path.read_text(encoding='utf-8'))
        with connect_source() as connection:
            connection.read_only = False
            saved = dict(entry, log_delivery_status='SAVED')
            connection.execute('INSERT INTO etl_ops.transformation_logs VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                               (entry['run_id'], entry['parent_run_id'], entry['ended_at'], Jsonb(saved)))
            for index, issue in enumerate(entry.get('issues', [])):
                if issue['severity'] == 'FAIL' and issue['record_id'] is not None:
                    connection.execute('INSERT INTO etl_ops.transformation_quarantine VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                                       (entry['run_id'], index, entry['ended_at'], Jsonb(issue)))
        path.unlink()


def save_logs(result):
    directory = pending_directory()
    descriptor, temporary = tempfile.mkstemp(dir=directory, suffix='.tmp')
    try:
        with os.fdopen(descriptor, 'w', encoding='utf-8') as handle:
            json.dump(result, handle)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, directory / (result['run_id'] + '.transformation'))
    finally:
        Path(temporary).unlink(missing_ok=True)
    flush_logs()


def main():
    if sys.argv[1:] not in ([], ['--flush-logs']):
        print('Usage: python -m pipeline.transformation [--flush-logs]')
        return 1
    flush_logs()
    if sys.argv[1:]:
        return 0
    consumer = TransformationConsumer()
    code = extraction.main(consumer)
    if consumer.result is not None:
        save_logs(consumer.result)
        print('Transformation logs saved. No permanent reporting table was loaded.')
        return code or {'PASS': 0, 'WARNING': 2, 'FAIL': 1}[consumer.result['status']]
    return code or 1


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except (psycopg.Error, SourceConnectionError, OSError, ValueError):
        print('Transformation operation failed. Check setup, database access, and pending metadata; no automatic replay was started.')
        raise SystemExit(1)
