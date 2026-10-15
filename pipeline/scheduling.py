"""Install and manage issue #10's database-only extraction schedule."""

import json
import sys

import psycopg
from psycopg import sql

from pipeline.connection import SourceConnectionError, connect_source
from pipeline.extraction import (
    EXTRACTION_LOCK_KEY, SOURCE_COLUMNS, setup_extraction_logs, validation_queries,
)


JOB_NAME = 'ic_laundry_daily_extraction'
JOB_COMMAND = 'CALL etl_ops.run_scheduled_extraction();'


def check_statement(table, name, status, affected='0'):
    return sql.SQL('''
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', {}, 'check', {}, 'status', {}, 'affected_rows', {}));
    ''').format(sql.Literal(table), sql.Literal(name), sql.SQL(status), sql.SQL(affected))


def procedure_sql():
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
            sql.Identifier('public', table), sql.Literal(table),
            sql.Identifier('pg_temp', 'stage_' + table), selected, selected,
            sql.Identifier('public', table),
            sql.Identifier('pg_temp', 'stage_' + table), sql.Literal(table),
        ))

    validations = []
    for table, name, query, warning in validation_queries():
        validations.append(sql.SQL('{} INTO affected;').format(query))
        status = "CASE WHEN affected = 0 THEN 'PASS' ELSE '{}' END".format(
            'WARNING' if warning else 'FAIL'
        )
        validations.append(check_statement(table, name, status, 'affected'))

    # Procedure transaction control requires a top-level CALL. Do not add a
    # SECURITY DEFINER or SET clause: those prohibit COMMIT inside a procedure.
    return sql.SQL('''
        CREATE OR REPLACE PROCEDURE etl_ops.run_scheduled_extraction()
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
                            SELECT udt_name INTO actual_type
                            FROM information_schema.columns
                            WHERE table_schema = 'public'
                              AND table_name = table_entry.key
                              AND column_name = column_entry.key;
                            IF actual_type IS DISTINCT FROM column_entry.value THEN
                                failure_code := 'SCHEMA_MISMATCH';
                                RAISE EXCEPTION 'Source schema does not match extraction contract';
                            END IF;
                            EXECUTE format('SELECT %I FROM public.%I LIMIT 0',
                                           column_entry.key, table_entry.key);
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
                    INSERT INTO etl_ops.extraction_logs (
                        pipeline_run_id, table_name, extraction_started_at, extraction_ended_at,
                        extraction_status, records_read, records_extracted, records_rejected,
                        extraction_window, validation_results, error_code, error_message,
                        duration_ms, acceptance_status
                    ) VALUES (
                        run_id, 'public.' || table_entry.key, started, ended, table_status,
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
        REVOKE ALL ON PROCEDURE etl_ops.run_scheduled_extraction() FROM PUBLIC;
    ''').format(
        sql.Literal(json.dumps(SOURCE_COLUMNS)),
        sql.SQL('\n').join(stages),
        check_statement('*', 'source_accessibility', "'PASS'"),
        sql.Literal(EXTRACTION_LOCK_KEY[0]), sql.Literal(EXTRACTION_LOCK_KEY[1]),
        check_statement('*', 'overlap_lock', "'PASS'"),
        check_statement('*', 'snapshot_consistency', "'PASS'"),
        check_statement('*', 'schema_compatibility', "'PASS'"),
        sql.SQL('\n').join(reads), sql.SQL('\n').join(validations),
        check_statement('*', 'completion', "'PASS'"),
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


def manage_schedule(action):
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
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 FROM pg_extension WHERE extname = 'pg_cron'")
            if not cursor.fetchone():
                raise ValueError('Enable pg_cron in Supabase before schedule setup.')
            expression = cron_expression(connection)
            if action == 'setup':
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
    if len(sys.argv) != 2 or sys.argv[1] not in ('setup','run','enable','disable','status'):
        print('Usage: python -m pipeline.scheduling setup|run|enable|disable|status')
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
