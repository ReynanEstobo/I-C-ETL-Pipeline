"""Install and manage issue #10's database-only extraction schedule."""

import json
import sys
from datetime import datetime, timezone
from uuid import uuid4

import psycopg
from psycopg import sql

from pipeline.connection import SourceConnectionError, connect_source
from pipeline.extraction import (
    EXTRACTION_LOCK_KEY, SOURCE_COLUMNS, setup_extraction_logs, validation_queries,
)


JOB_NAME = 'ic_laundry_daily_extraction'
JOB_COMMAND = 'CALL etl_ops.run_scheduled_extraction();'


def log_recovery_sql(schema='etl_ops'):
    """Durable database metadata queue and atomic, duplicate-safe delivery."""
    columns = sql.SQL(', ').join(map(sql.Identifier, (
        'pipeline_run_id', 'table_name', 'extraction_started_at', 'extraction_ended_at',
        'extraction_status', 'records_read', 'records_extracted', 'records_rejected',
        'extraction_window', 'validation_results', 'error_code', 'error_message',
        'duration_ms', 'acceptance_status',
    )))
    pending = sql.Identifier(schema, 'pending_extraction_logs')
    logs = sql.Identifier(schema, 'extraction_logs')
    function = sql.Identifier(schema, 'recover_extraction_logs')
    return sql.SQL('''
        CREATE TABLE IF NOT EXISTS {} (
            LIKE {} INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
        );
        REVOKE ALL ON {} FROM PUBLIC;
        ALTER TABLE {} ENABLE ROW LEVEL SECURITY;
        CREATE OR REPLACE FUNCTION {}(requested_run uuid DEFAULT NULL)
        RETURNS bigint LANGUAGE plpgsql SECURITY INVOKER AS $recover$
        DECLARE delivered bigint;
        BEGIN
            -- Serialize metadata replay independently from extraction's lock.
            PERFORM pg_advisory_xact_lock(18755, 1163152461);
            INSERT INTO {} ({})
                SELECT {} FROM {}
                WHERE requested_run IS NULL OR pipeline_run_id = requested_run
                ON CONFLICT (pipeline_run_id, table_name) DO NOTHING;
            DELETE FROM {} pending
                WHERE (requested_run IS NULL OR pending.pipeline_run_id = requested_run)
                  AND EXISTS (SELECT 1 FROM {} saved
                    WHERE saved.pipeline_run_id = pending.pipeline_run_id
                      AND saved.table_name = pending.table_name);
            GET DIAGNOSTICS delivered = ROW_COUNT;
            RETURN delivered;
        END;
        $recover$;
        REVOKE ALL ON FUNCTION {}(uuid) FROM PUBLIC;
    ''').format(pending, logs, pending, pending, function,
                logs, columns, columns, pending, pending, logs, function)


def check_statement(table, name, status, affected='0'):
    return sql.SQL('''
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', {}, 'check', {}, 'status', {}, 'affected_rows', {}));
    ''').format(sql.Literal(table), sql.Literal(name), sql.SQL(status), sql.SQL(affected))


def procedure_sql(source_schema='public', procedure_schema='etl_ops'):
    """Generate SQL from the extractor's column contract and check queries."""
    stages = []
    reads = []
    for table, columns in SOURCE_COLUMNS.items():
        definitions = sql.SQL(', ').join(
            sql.SQL('{} {}').format(sql.Identifier(column), sql.SQL(kind))
            for column, kind in columns.items()
        )
        stages.append(sql.SQL(
            'CREATE TEMP TABLE {} ({}) ON COMMIT DELETE ROWS;'
        ).format(sql.Identifier('stage_' + table), definitions))
        selected = sql.SQL(', ').join(map(sql.Identifier, columns))
        reads.append(sql.SQL('''
            SELECT count(*) INTO n FROM {};
            source_counts := source_counts || jsonb_build_object({}, n);
            INSERT INTO {} ({}) SELECT {} FROM {};
            SELECT count(*) INTO n FROM {};
            staged_counts := staged_counts || jsonb_build_object({}, n);
        ''').format(
            sql.Identifier(source_schema, table), sql.Literal(table),
            sql.Identifier('pg_temp', 'stage_' + table), selected, selected,
            sql.Identifier(source_schema, table),
            sql.Identifier('pg_temp', 'stage_' + table), sql.Literal(table),
        ))

    validations = []
    for table, name, query, warning in validation_queries(source_schema):
        validations.append(sql.SQL('{} INTO affected;').format(query))
        status = "CASE WHEN affected = 0 THEN 'PASS' ELSE '{}' END".format(
            'WARNING' if warning else 'FAIL'
        )
        validations.append(check_statement(table, name, status, 'affected'))

    # Procedure transaction control requires a top-level CALL. Do not add a
    # SECURITY DEFINER or SET clause: those prohibit COMMIT inside a procedure.
    return sql.SQL('''
        CREATE OR REPLACE PROCEDURE {}()
        LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
        DECLARE
            run_id uuid;
            started timestamptz;
            ended timestamptz;
            checks jsonb;
            source_counts jsonb;
            staged_counts jsonb;
            expected jsonb := {}::jsonb;
            table_entry record;
            column_entry record;
            actual_type text;
            n bigint;
            affected bigint;
            overall text;
            table_status text;
            acceptance text;
            phase text;
            failure_code text;
            failure_message text;
            failure_state text;
            may_retry boolean;
            original_isolation text := current_setting('default_transaction_isolation');
            original_read_only text := current_setting('default_transaction_read_only');
        BEGIN
            -- Empty temporary structures only; source reads start after COMMIT.
            SET TRANSACTION READ WRITE;
            PERFORM etl_ops.recover_extraction_logs();
            {}
            SET SESSION CHARACTERISTICS AS TRANSACTION
                ISOLATION LEVEL REPEATABLE READ, READ ONLY;
            COMMIT;

            FOR attempt IN 1..3 LOOP
                run_id := gen_random_uuid();
                started := clock_timestamp();
                checks := '[]'::jsonb;
                source_counts := '{{}}'::jsonb;
                staged_counts := '{{}}'::jsonb;
                failure_code := NULL;
                failure_message := NULL;
                failure_state := NULL;
                may_retry := false;
                overall := 'PASS';
                {}

                -- The exception subtransaction rolls back staged writes but
                -- retains local metadata for the following logging transaction.
                BEGIN
                    phase := 'overlap_lock';
                    IF NOT pg_try_advisory_xact_lock({}, {}) THEN
                        failure_code := 'EXTRACTION_ALREADY_RUNNING';
                        RAISE EXCEPTION 'Extraction already running';
                    END IF;
                    {}
                    phase := 'snapshot_consistency';
                    IF current_setting('transaction_read_only') <> 'on'
                       OR current_setting('transaction_isolation') <> 'repeatable read' THEN
                        RAISE EXCEPTION 'Invalid extraction transaction settings';
                    END IF;
                    {}

                    phase := 'schema_compatibility';
                    FOR table_entry IN SELECT * FROM jsonb_each(expected) LOOP
                        FOR column_entry IN SELECT * FROM jsonb_each_text(table_entry.value) LOOP
                            SELECT t.typname INTO actual_type
                            FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace ns ON ns.oid = c.relnamespace
                            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                            WHERE ns.nspname = {} AND c.relname = table_entry.key
                              AND a.attname = column_entry.key
                              AND a.attnum > 0 AND NOT a.attisdropped
                              AND c.relkind IN ('r', 'p');
                            IF actual_type IS DISTINCT FROM column_entry.value THEN
                                failure_code := 'SCHEMA_MISMATCH';
                                RAISE EXCEPTION 'Source schema does not match extraction contract';
                            END IF;
                        END LOOP;
                    END LOOP;
                    {}
                    phase := 'source_accessibility';
                    FOR table_entry IN SELECT * FROM jsonb_each(expected) LOOP
                        FOR column_entry IN SELECT * FROM jsonb_each_text(table_entry.value) LOOP
                            EXECUTE format('SELECT %I FROM %I.%I LIMIT 0',
                                           column_entry.key, {}, table_entry.key);
                        END LOOP;
                    END LOOP;
                    {}
                    phase := 'completion';
                    {}
                    {}
                    {}

                    SELECT CASE WHEN bool_or(c->>'status' = 'FAIL') THEN 'FAIL'
                                WHEN bool_or(c->>'status' = 'WARNING') THEN 'WARNING'
                                ELSE 'PASS' END
                    INTO overall FROM jsonb_array_elements(checks) c;
                    IF overall <> 'PASS' THEN
                        -- Roll back staged records without treating warnings
                        -- as execution failures or retrying data problems.
                        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Acceptance blocked';
                    END IF;
                    -- Extraction-only check: no downstream stage is invoked.
                EXCEPTION WHEN OTHERS THEN
                    GET STACKED DIAGNOSTICS failure_state = RETURNED_SQLSTATE;
                    IF failure_state = 'P0002' AND overall <> 'PASS' THEN
                        IF overall = 'FAIL' THEN
                            failure_code := 'VALIDATION_FAILED';
                            failure_message := 'One or more extraction checks failed.';
                        END IF;
                    ELSE
                        overall := 'FAIL';
                        failure_code := coalesce(failure_code, 'EXTRACTION_FAILED');
                        failure_message := CASE WHEN failure_code = 'EXTRACTION_ALREADY_RUNNING'
                            THEN 'Another extraction holds the advisory lock. No source records were read.'
                            ELSE 'Extraction or SQL validation could not complete.' END;
                        checks := coalesce((SELECT jsonb_agg(c)
                            FROM jsonb_array_elements(checks) c
                            WHERE NOT (c->>'table' = '*' AND c->>'check' IN (phase, 'completion'))),
                            '[]'::jsonb);
                        checks := checks || jsonb_build_array(
                            jsonb_build_object('table','*','check',phase,'status','FAIL','affected_rows',1));
                        IF phase <> 'completion' THEN
                            checks := checks || jsonb_build_array(jsonb_build_object(
                                'table','*','check','completion','status','FAIL','affected_rows',1));
                        END IF;
                        may_retry := failure_state LIKE '08%%'
                            OR failure_state IN ('40001','40P01','53300','57P01','57P02','57P03');
                    END IF;
                END;
                ended := clock_timestamp();
                acceptance := CASE overall WHEN 'PASS' THEN 'APPROVED'
                                          WHEN 'WARNING' THEN 'MANUAL REVIEW'
                                          ELSE 'REJECTED' END;
                SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;
                COMMIT;

                -- Permanent writes occur only after the source snapshot ends.
                FOR table_entry IN SELECT * FROM jsonb_each(expected) LOOP
                    SELECT CASE WHEN bool_or(c->>'status' = 'FAIL') THEN 'FAIL'
                                WHEN bool_or(c->>'status' = 'WARNING') THEN 'WARNING'
                                ELSE 'PASS' END INTO table_status
                    FROM jsonb_array_elements(checks) c
                    WHERE c->>'table' IN (table_entry.key, '*');
                    INSERT INTO etl_ops.pending_extraction_logs (
                        pipeline_run_id, table_name, extraction_started_at, extraction_ended_at,
                        extraction_status, records_read, records_extracted, records_rejected,
                        extraction_window, validation_results, error_code, error_message,
                        duration_ms, acceptance_status
                    ) VALUES (
                        run_id, {} || '.' || table_entry.key, started, ended, table_status,
                        (source_counts->>table_entry.key)::bigint,
                        (staged_counts->>table_entry.key)::bigint, 0, 'All available history',
                        jsonb_build_object('overall_status', overall, 'checks',
                            (SELECT coalesce(jsonb_agg(c), '[]'::jsonb)
                             FROM jsonb_array_elements(checks) c
                             WHERE c->>'table' IN (table_entry.key, '*'))),
                        failure_code, failure_message,
                        floor(extract(epoch FROM ended - started) * 1000)::bigint, acceptance
                    ) ON CONFLICT (pipeline_run_id, table_name) DO NOTHING;
                END LOOP;
                -- Queue metadata durably before attempting final-log delivery.
                COMMIT;
                PERFORM etl_ops.recover_extraction_logs(run_id);
                SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;
                COMMIT;
                RAISE NOTICE 'Extraction run %: %', run_id, acceptance;
                IF NOT may_retry OR attempt = 3 THEN
                    PERFORM set_config('default_transaction_isolation', original_isolation, false);
                    PERFORM set_config('default_transaction_read_only', original_read_only, false);
                    COMMIT;
                    IF overall = 'FAIL' THEN
                        RAISE EXCEPTION 'Scheduled extraction rejected; inspect etl_ops.extraction_logs';
                    END IF;
                    RETURN;
                END IF;
                PERFORM pg_sleep(attempt * 2);
                COMMIT;
            END LOOP;
        END;
        $procedure$;
        REVOKE ALL ON PROCEDURE {}() FROM PUBLIC;
    ''').format(
        sql.Identifier(procedure_schema, 'run_scheduled_extraction'),
        sql.Literal(json.dumps(SOURCE_COLUMNS)),
        sql.SQL('\n').join(stages),
        check_statement('*', 'source_connection', "'PASS'"),
        sql.Literal(EXTRACTION_LOCK_KEY[0]), sql.Literal(EXTRACTION_LOCK_KEY[1]),
        check_statement('*', 'overlap_lock', "'PASS'"),
        check_statement('*', 'snapshot_consistency', "'PASS'"),
        sql.Literal(source_schema),
        check_statement('*', 'schema_compatibility', "'PASS'"),
        sql.Literal(source_schema),
        check_statement('*', 'source_accessibility', "'PASS'"),
        sql.SQL('\n').join(reads), sql.SQL('\n').join(validations),
        check_statement('*', 'completion', "'PASS'"),
        sql.Literal(source_schema),
        sql.Identifier(procedure_schema, 'run_scheduled_extraction'),
    )


def cron_expression(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_setting('cron.timezone', true)")
        zone = cursor.fetchone()[0] or 'GMT'
    if zone in ('GMT', 'UTC', 'Etc/UTC', 'Etc/GMT'):
        return '0 17 * * *'
    if zone in ('Asia/Manila', 'Asia/Singapore'):
        return '0 1 * * *'
    raise ValueError('Unsupported cron.timezone; verify scheduler timezone before setup.')


def test_scheduled_extraction():
    """Exercise the production SQL generator with isolated synthetic sources."""
    test_schema = 'etl_test_' + uuid4().hex
    print(f'Isolated test schema: {test_schema}')
    with connect_source() as connection:
        connection.read_only = False
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(test_schema)))
                cursor.execute(sql.SQL('REVOKE ALL ON SCHEMA {} FROM PUBLIC').format(
                    sql.Identifier(test_schema)))
                for table, columns in SOURCE_COLUMNS.items():
                    definitions = sql.SQL(', ').join(
                        sql.SQL('{} {}').format(sql.Identifier(column), sql.SQL(kind))
                        for column, kind in columns.items()
                    )
                    cursor.execute(sql.SQL('CREATE TABLE {} ({})').format(
                        sql.Identifier(test_schema, table), definitions))
                cursor.execute(procedure_sql(test_schema, test_schema))
            connection.commit()
            connection.autocommit = True
            # Direct fixture writes need writable autocommit transactions.
            connection.execute('SET default_transaction_read_only = off')
            branch, customer, order, payment, expense = [uuid4() for _ in range(5)]
            now = datetime.now(timezone.utc)
            fixtures = {
                'branches': [branch, 'Test branch', None, None],
                'customers': [customer],
                'orders': [order, 'TEST-001', branch, customer, now, 'received', 8, 200],
                'payments': [payment, order, branch, 100, now, 'test'],
                'expenses': [expense, branch, 50, now.date(), None],
            }

            def reset_fixtures():
                for table, values in fixtures.items():
                    connection.execute(sql.SQL('DELETE FROM {}').format(
                        sql.Identifier(test_schema, table)))
                    connection.execute(sql.SQL('INSERT INTO {} VALUES ({})').format(
                        sql.Identifier(test_schema, table),
                        sql.SQL(', ').join(sql.Placeholder() for _ in values)), values)

            def run_case(label, expected, expected_error=None, expected_check=None):
                try:
                    connection.execute(sql.SQL('CALL {}()').format(
                        sql.Identifier(test_schema, 'run_scheduled_extraction')))
                    assert expected != 'REJECTED', f'{label}: expected rejection'
                except psycopg.Error as error:
                    if not (expected == 'REJECTED' and error.sqlstate == 'P0001'
                            and error.diag.message_primary.startswith('Scheduled extraction rejected')):
                        raise
                rows = connection.execute('''
                    SELECT pipeline_run_id, table_name, extraction_status,
                           acceptance_status, error_code, records_read,
                           records_extracted, validation_results
                    FROM etl_ops.extraction_logs
                    WHERE pipeline_run_id = (
                        SELECT pipeline_run_id FROM etl_ops.extraction_logs
                        WHERE split_part(table_name, '.', 1) = %s
                        ORDER BY extraction_started_at DESC LIMIT 1)
                ''', (test_schema,)).fetchall()
                assert len(rows) == 5, f'{label}: missing table logs'
                assert all(row[3] == expected for row in rows), f'{label}: acceptance mismatch'
                assert all(row[4] == expected_error for row in rows), f'{label}: error code mismatch'
                if expected_check:
                    assert any(check['check'] == expected_check and check['status'] != 'PASS'
                               for row in rows for check in row[7]['checks']), label
                if expected_error == 'EXTRACTION_ALREADY_RUNNING':
                    assert all(row[5] is None and row[6] is None for row in rows)
                # COMMIT clears successful stages; rejected/held stages roll back.
                for table in SOURCE_COLUMNS:
                    count = connection.execute(sql.SQL('SELECT count(*) FROM {}').format(
                        sql.Identifier('pg_temp', 'stage_' + table))).fetchone()[0]
                    assert count == 0, f'{label}: staging was not cleared'
                    connection.execute(sql.SQL('DROP TABLE {}').format(
                        sql.Identifier('pg_temp', 'stage_' + table)))
                print(f'PASS: {label} ({rows[0][0]})')

            reset_fixtures()
            run_case('Valid populated records', 'APPROVED')
            connection.execute(sql.SQL('UPDATE {} SET branch_id = NULL').format(
                sql.Identifier(test_schema, 'orders')))
            run_case('Nullable branch warning', 'MANUAL REVIEW', expected_check='branch_id_present')
            reset_fixtures()
            connection.execute(sql.SQL('UPDATE {} SET paid_at = NULL').format(
                sql.Identifier(test_schema, 'payments')))
            run_case('Missing required date', 'REJECTED', 'VALIDATION_FAILED', 'paid_at_present')
            reset_fixtures()
            connection.execute(sql.SQL('UPDATE {} SET customer_id = %s').format(
                sql.Identifier(test_schema, 'orders')), (uuid4(),))
            run_case('Broken customer reference', 'REJECTED', 'VALIDATION_FAILED', 'customer_id_reference')
            reset_fixtures()
            connection.execute(sql.SQL('INSERT INTO {} SELECT * FROM {}').format(
                sql.Identifier(test_schema, 'orders'), sql.Identifier(test_schema, 'orders')))
            run_case('Duplicate identifiers', 'REJECTED', 'VALIDATION_FAILED', 'id_unique')
            reset_fixtures()
            with connect_source() as holder:
                holder.execute('SELECT pg_advisory_xact_lock(%s, %s)', EXTRACTION_LOCK_KEY)
                run_case('Overlapping run', 'REJECTED', 'EXTRACTION_ALREADY_RUNNING', 'overlap_lock')
            run_case('Lock released', 'APPROVED')
            print('All scheduled extraction tests passed. Synthetic test logs are retained.')
        finally:
            if not connection.closed:
                connection.rollback()
                connection.autocommit = True
                connection.execute('SET default_transaction_read_only = off')
                # This name is generated locally, not supplied by the user.
                assert test_schema.startswith('etl_test_') and len(test_schema) == 41
                connection.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(
                    sql.Identifier(test_schema)))
                print('Isolated test schema removed. Operational tables and Cron jobs were unchanged.')


def test_scheduled_log_recovery():
    """Test the real delivery function against private synthetic log tables."""
    schema = 'etl_test_' + uuid4().hex
    with connect_source() as connection:
        connection.read_only = False
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(schema)))
                cursor.execute(sql.SQL('REVOKE ALL ON SCHEMA {} FROM PUBLIC').format(sql.Identifier(schema)))
                cursor.execute(sql.SQL('CREATE TABLE {} (LIKE etl_ops.extraction_logs INCLUDING ALL)').format(
                    sql.Identifier(schema, 'extraction_logs')))
                cursor.execute(log_recovery_sql(schema))
                cursor.execute(sql.SQL('''
                    CREATE FUNCTION {}() RETURNS trigger LANGUAGE plpgsql AS $test$
                    BEGIN RAISE EXCEPTION 'Simulated final-log write failure'; END;
                    $test$;
                    CREATE TRIGGER fail_delivery BEFORE INSERT ON {}
                        FOR EACH ROW EXECUTE FUNCTION {}();
                ''').format(sql.Identifier(schema, 'fail_delivery'),
                            sql.Identifier(schema, 'extraction_logs'),
                            sql.Identifier(schema, 'fail_delivery')))
                run_id = uuid4()
                timestamp = datetime.now(timezone.utc)
                cursor.execute(sql.SQL('''INSERT INTO {} (
                    pipeline_run_id, table_name, extraction_started_at, extraction_ended_at,
                    extraction_status, records_read, records_extracted, extraction_window,
                    validation_results, duration_ms, acceptance_status
                ) VALUES (%s, 'test.orders', %s, %s, 'FAIL', NULL, NULL,
                          'All available history', '{{}}'::jsonb, 0, 'REJECTED')''').format(
                    sql.Identifier(schema, 'pending_extraction_logs')),
                    (run_id, timestamp, timestamp))
            connection.commit()
            recover = sql.SQL('SELECT {}()').format(sql.Identifier(schema, 'recover_extraction_logs'))
            try:
                connection.execute(recover)
                raise AssertionError('Simulated log-write failure was not raised')
            except psycopg.Error as error:
                assert error.sqlstate == 'P0001'
                connection.rollback()
            pending = sql.Identifier(schema, 'pending_extraction_logs')
            assert connection.execute(sql.SQL('SELECT count(*) FROM {}').format(pending)).fetchone()[0] == 1
            print('PASS: committed pending metadata survived final-log rollback')
            connection.execute(sql.SQL('DROP TRIGGER fail_delivery ON {}').format(
                sql.Identifier(schema, 'extraction_logs')))
            connection.commit()
            assert connection.execute(recover).fetchone()[0] == 1
            connection.commit()
            logs = sql.Identifier(schema, 'extraction_logs')
            saved = connection.execute(sql.SQL('SELECT pipeline_run_id, extraction_started_at FROM {}').format(logs)).fetchone()
            assert saved == (run_id, timestamp)
            print('PASS: original run ID and timestamp preserved after recovery')
            assert connection.execute(sql.SQL('SELECT count(*) FROM {}').format(pending)).fetchone()[0] == 0
            # Simulate uncertain delivery by replaying an already committed row.
            connection.execute(sql.SQL('INSERT INTO {} SELECT * FROM {}').format(pending, logs))
            connection.commit()
            assert connection.execute(recover).fetchone()[0] == 1
            assert connection.execute(sql.SQL('SELECT count(*) FROM {}').format(logs)).fetchone()[0] == 1
            connection.commit()
            print('PASS: replay removed confirmed metadata without duplicate logs')
        finally:
            connection.rollback()
            connection.read_only = False
            connection.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(schema)))
            connection.commit()
            print('Recovery test schema removed; operational tables and logs were unchanged.')


def manage_schedule(action):
    if action == 'test-recovery':
        test_scheduled_log_recovery()
        return
    if action == 'test':
        test_scheduled_extraction()
        return
    if action == 'setup':
        setup_extraction_logs()
    with connect_source() as connection:
        connection.read_only = False
        if action == 'run':
            # Procedures that commit cannot be called inside a client transaction.
            connection.autocommit = True
            with connection.cursor() as cursor:
                cursor.execute(JOB_COMMAND)
            print('Database extraction check completed. Inspect extraction logs.')
            return
        if action == 'update':
            with connection.cursor() as cursor:
                cursor.execute(log_recovery_sql())
                cursor.execute(procedure_sql())
            print('Database procedure updated. Cron schedule and active status were unchanged.')
            return
        if action == 'recover':
            with connection.cursor() as cursor:
                cursor.execute('SELECT etl_ops.recover_extraction_logs()')
                print(f'Recovered {cursor.fetchone()[0]} pending table-log rows.')
            return
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'")
            if not cursor.fetchone():
                raise ValueError('Enable pg_cron in Supabase before schedule setup.')
            expression = cron_expression(connection)
            if action == 'setup':
                cursor.execute(log_recovery_sql())
                cursor.execute(procedure_sql())
                cursor.execute('SELECT cron.schedule(%s, %s, %s)',
                               (JOB_NAME, expression, JOB_COMMAND))
                job_id = cursor.fetchone()[0]
                cursor.execute('SELECT cron.alter_job(%s, active := false)', (job_id,))
                print(f'Job {job_id} configured but disabled: {expression}.')
            elif action in ('enable', 'disable'):
                cursor.execute('SELECT jobid, schedule, command FROM cron.job WHERE jobname = %s',
                               (JOB_NAME,))
                job = cursor.fetchone()
                if not job:
                    raise ValueError('Run schedule setup first.')
                if job[1:] != (expression, JOB_COMMAND):
                    raise ValueError('Job configuration changed; run setup and verify before enabling.')
                cursor.execute('SELECT cron.alter_job(%s, active := %s)',
                               (job[0], action == 'enable'))
                print(f'Extraction schedule {action}d.')
            elif action == 'status':
                cursor.execute('SELECT jobid, schedule, active FROM cron.job WHERE jobname = %s',
                               (JOB_NAME,))
                print('Job:', cursor.fetchone())
                cursor.execute('''SELECT status, start_time, end_time FROM cron.job_run_details
                    WHERE jobid IN (SELECT jobid FROM cron.job WHERE jobname = %s)
                    ORDER BY start_time DESC LIMIT 5''', (JOB_NAME,))
                for row in cursor.fetchall():
                    print('History:', row)


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ('setup','run','enable','disable','status','test','update','recover','test-recovery'):
        print('Usage: python -m pipeline.scheduling setup|run|enable|disable|status|test|update|recover|test-recovery')
        return 1
    try:
        manage_schedule(sys.argv[1])
    except ValueError as error:
        print(error)
        return 1
    except psycopg.Error as error:
        print(f'Scheduling operation failed (SQLSTATE: {error.sqlstate or "unavailable"}).')
        if error.sqlstate == '25001':
            print(error.diag.message_primary)
            if error.diag.context:
                print(error.diag.context)
        print('Check permissions, pg_cron, and database logs.')
        return 1
    except SourceConnectionError:
        print('Scheduling operation failed. Check permissions, pg_cron, and database logs.')
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
