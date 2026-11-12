"""Install, run, and manage the scheduled pipeline (Supabase Cron / pg_cron)."""

import json
import sys

import psycopg
from psycopg import sql

from pipeline import report
from pipeline.connection import SourceConnectionError, connect_source
from pipeline.extraction import (
    EXTRACTION_LOCK_KEY, SOURCE_COLUMNS, setup_extraction_logs, validation_queries,
)


SOURCE_SCHEMA = 'public'
OPS_SCHEMA = 'etl_ops'
JOB_NAME = 'ic_laundry_daily_extraction'
# The job runs extraction and Stage 2, then publishes the load in the same session.
JOB_COMMAND = 'CALL etl_ops.run_scheduled_pipeline();'


def load_handover_consumer_sql():
    """Install the Stage 2-to-Load handover contract repair."""
    return '''
        CREATE OR REPLACE FUNCTION etl_ops.consume_transformation_with_load(payload jsonb)
        RETURNS jsonb
        LANGUAGE plpgsql
        AS $function$
        DECLARE
            v_response JSONB;
            v_run JSONB;
            v_offered JSONB;
            v_answer JSONB;
            v_handover TEXT := 'NOT_CONFIGURED';
            v_acknowledge REGPROCEDURE;
            v_consume REGPROCEDURE;
            v_schema TEXT;
            v_name TEXT;
        BEGIN
            v_response := etl_ops.consume_transformation(payload);
            v_run := v_response->'transformation_run';
            IF v_run IS NULL OR coalesce(v_run->>'status', '') NOT IN ('PASS', 'PARTIAL') THEN
                RETURN v_response;
            END IF;

            SELECT h.acknowledge_function, h.consume_function INTO v_acknowledge, v_consume
            FROM etl_ops.transformation_handover h;
            IF v_acknowledge IS NOT NULL THEN
                -- Pass the extraction identity through unchanged for Load.
                v_offered := v_run || jsonb_build_object(
                    'output', 'pg_temp.transformed_daily_branch_summary');
                IF NOT v_offered ? 'parent_run_id' THEN
                    v_offered := v_offered || jsonb_build_object(
                        'parent_run_id', payload->'pipeline_run_id');
                END IF;
                IF NOT v_offered ? 'extraction_started_at' THEN
                    v_offered := v_offered || jsonb_build_object(
                        'extraction_started_at', payload->'extraction_started_at');
                END IF;
                -- Load requires a Stage 2 window with an exclusive Manila-date cutoff.
                -- Do not replace a window emitted by a newer transformation function.
                IF NOT v_offered ? 'batch_window'
                   OR v_offered->'batch_window' IS NULL
                   OR v_offered->>'batch_window' = '' THEN
                    v_offered := v_offered || jsonb_build_object(
                        'batch_window', jsonb_build_object(
                            'start', (SELECT min(report_date)
                                      FROM pg_temp.transformed_daily_branch_summary),
                            'cutoff', ((payload->>'extraction_started_at')::timestamptz
                                       AT TIME ZONE 'Asia/Manila')::date));
                END IF;
                BEGIN
                    v_handover := 'FAILED';
                    FOR receiver IN 1..2 LOOP
                        v_name := NULL;
                        SELECT ns.nspname, p.proname INTO v_schema, v_name
                        FROM pg_catalog.pg_proc p
                        JOIN pg_catalog.pg_namespace ns ON ns.oid = p.pronamespace
                        WHERE p.oid = CASE receiver WHEN 1 THEN v_acknowledge::oid
                                                    ELSE v_consume::oid END
                          AND p.prokind = 'f' AND p.pronargs = 1
                          AND p.proargtypes[0] = 'jsonb'::regtype
                          AND p.prorettype = 'jsonb'::regtype
                          AND NOT p.prosecdef;
                        IF v_name IS NULL THEN
                            RAISE EXCEPTION 'Load receivers must be invoker functions accepting and returning JSONB';
                        END IF;
                        EXECUTE format('SELECT %I.%I($1)', v_schema, v_name)
                            INTO v_answer USING v_offered;
                        IF v_answer ? 'rejected' THEN
                            EXIT;
                        END IF;
                        IF receiver = 1 AND (v_answer->>'run_id' IS DISTINCT FROM v_offered->>'run_id'
                            OR v_answer->>'acceptance_status' IS DISTINCT FROM v_offered->>'acceptance_status') THEN
                            EXIT;
                        END IF;
                        IF receiver = 2 THEN
                            v_handover := 'ACKNOWLEDGED';
                        END IF;
                    END LOOP;
                EXCEPTION WHEN OTHERS THEN
                    v_handover := 'FAILED';
                END;
            END IF;
            RETURN jsonb_set(v_response, '{transformation_run,handover}', to_jsonb(v_handover));
        END;
        $function$;
    '''


def log_recovery_sql():
    """Durable database metadata queue and atomic, duplicate-safe delivery."""
    columns = sql.SQL(', ').join(map(sql.Identifier, (
        'pipeline_run_id', 'table_name', 'extraction_started_at', 'extraction_ended_at',
        'extraction_status', 'records_read', 'records_extracted', 'records_rejected',
        'extraction_window', 'validation_results', 'error_code', 'error_message',
        'duration_ms', 'acceptance_status',
    )))
    pending = sql.Identifier(OPS_SCHEMA, 'pending_extraction_logs')
    logs = sql.Identifier(OPS_SCHEMA, 'extraction_logs')
    function = sql.Identifier(OPS_SCHEMA, 'recover_extraction_logs')
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


def handover_config_sql():
    """No receiver is installed by default; only the owner can configure one."""
    table = sql.Identifier(OPS_SCHEMA, 'extraction_handover')
    return sql.SQL('''
        CREATE TABLE IF NOT EXISTS {} (
            singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
            acknowledge_function regprocedure NOT NULL,
            consume_function regprocedure NOT NULL
        );
        REVOKE ALL ON {} FROM PUBLIC;
        ALTER TABLE {} ENABLE ROW LEVEL SECURITY;
    ''').format(table, table, table)


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
            sql.Identifier(SOURCE_SCHEMA, table), sql.Literal(table),
            sql.Identifier('pg_temp', 'stage_' + table), selected, selected,
            sql.Identifier(SOURCE_SCHEMA, table),
            sql.Identifier('pg_temp', 'stage_' + table), sql.Literal(table),
        ))

    validations = []
    stages.append(sql.SQL('''
        IF to_regprocedure('etl_ops.prepare_transformation()') IS NOT NULL THEN
            PERFORM etl_ops.recover_transformation_logs();
            PERFORM etl_ops.prepare_transformation();
        END IF;
    '''))
    for table, name, query, warning in validation_queries():
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
            handover_acceptance text;
            phase text;
            failure_code text;
            failure_message text;
            failure_state text;
            may_retry boolean;
            acknowledge_function regprocedure;
            consume_function regprocedure;
            receiver_schema text;
            receiver_name text;
            handover_payload jsonb;
            acknowledgment jsonb;
            receiver_result jsonb;
            table_results jsonb;
            stage_names jsonb;
            handover_status text;
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
                receiver_result := NULL;
                failure_state := NULL;
                may_retry := false;
                overall := 'PASS';
                handover_status := 'NOT_ATTEMPTED';
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
                    IF overall = 'FAIL' THEN
                        -- Failed extraction checks cannot be handed to Stage 2.
                        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Acceptance blocked';
                    END IF;
                    handover_acceptance := CASE overall WHEN 'PASS' THEN 'APPROVED'
                                                        ELSE 'APPROVED_WITH_WARNINGS' END;
                    phase := 'handover';
                    SELECT h.acknowledge_function, h.consume_function
                    INTO acknowledge_function, consume_function FROM {} h;
                    IF acknowledge_function IS NULL THEN
                        handover_status := 'NOT_CONFIGURED';
                    END IF;
                    IF acknowledge_function IS NOT NULL THEN
                        -- Hooks run in this snapshot; they cannot commit and
                        -- cannot write to permanent tables in read-only mode.
                        handover_status := 'FAILED';
                        failure_code := 'HANDOVER_FAILED';
                        SELECT jsonb_object_agg(key, 'pg_temp.stage_' || key)
                            INTO stage_names FROM jsonb_each(expected);
                        SELECT jsonb_object_agg(key, 'PASS')
                            INTO table_results FROM jsonb_each(expected);
                        handover_payload := jsonb_build_object(
                            'pipeline_run_id', run_id, 'acceptance_status', handover_acceptance,
                            'extraction_status', overall, 'extraction_started_at', started,
                            'extraction_ended_at', clock_timestamp(),
                            'duration_ms', floor(extract(epoch FROM clock_timestamp()-started)*1000)::bigint,
                            'records_read', source_counts, 'records_extracted', staged_counts,
                            'records_rejected', (SELECT jsonb_object_agg(key, 0) FROM jsonb_each(expected)),
                            'extraction_window', 'All available history', 'stages', stage_names,
                            'validation_results', jsonb_build_object('status', overall,
                                'tables', table_results, 'checks', checks),
                            'error_code', NULL, 'error_message', NULL);
                        FOR receiver IN 1..2 LOOP
                            SELECT ns.nspname, p.proname INTO receiver_schema, receiver_name
                            FROM pg_catalog.pg_proc p
                            JOIN pg_catalog.pg_namespace ns ON ns.oid = p.pronamespace
                            WHERE p.oid = CASE receiver WHEN 1 THEN acknowledge_function::oid
                                                       ELSE consume_function::oid END
                              AND p.prokind = 'f' AND p.pronargs = 1
                              AND p.proargtypes[0] = 'jsonb'::regtype
                              AND p.prorettype = 'jsonb'::regtype
                              AND NOT p.prosecdef;
                            IF receiver_name IS NULL THEN
                                RAISE EXCEPTION 'Handover requires invoker functions accepting and returning JSONB';
                            END IF;
                            IF receiver = 1 THEN
                                EXECUTE format('SELECT %I.%I($1)', receiver_schema, receiver_name)
                                    INTO acknowledgment USING jsonb_build_object(
                                        'pipeline_run_id', run_id, 'acceptance_status', handover_acceptance);
                                IF acknowledgment->>'pipeline_run_id' IS DISTINCT FROM run_id::text
                                   OR acknowledgment->>'acceptance_status' IS DISTINCT FROM handover_acceptance THEN
                                    RAISE EXCEPTION 'Next stage did not acknowledge the extraction run';
                                END IF;
                            ELSE
                                EXECUTE format('SELECT %I.%I($1)', receiver_schema, receiver_name)
                                    INTO receiver_result USING handover_payload;
                            END IF;
                        END LOOP;
                        handover_status := 'ACKNOWLEDGED';
                        failure_code := NULL;
                        {}
                    END IF;
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
                        may_retry := phase <> 'handover' AND (failure_state LIKE '08%%'
                            OR failure_state IN ('40001','40P01','53300','57P01','57P02','57P03'));
                    END IF;
                END;
                ended := clock_timestamp();
                acceptance := CASE overall WHEN 'PASS' THEN 'APPROVED'
                                          WHEN 'WARNING' THEN 'APPROVED_WITH_WARNINGS'
                                          ELSE 'REJECTED' END;
                SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;
                COMMIT;

                -- Permanent writes occur only after the source snapshot ends.
                IF receiver_result ? 'transformation_run' THEN
                    PERFORM etl_ops.queue_transformation_log(receiver_result->'transformation_run');
                END IF;
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
                        jsonb_build_object('overall_status', overall, 'handover_status', handover_status, 'checks',
                            (SELECT coalesce(jsonb_agg(c), '[]'::jsonb)
                             FROM jsonb_array_elements(checks) c
                             WHERE c->>'table' IN (table_entry.key, '*'))),
                        failure_code, failure_message,
                        floor(extract(epoch FROM ended - started) * 1000)::bigint, acceptance
                    ) ON CONFLICT (pipeline_run_id, table_name) DO NOTHING;
                END LOOP;
                -- Queue metadata durably before attempting final-log delivery.
                COMMIT;
                IF to_regprocedure('etl_ops.recover_transformation_logs()') IS NOT NULL THEN
                    PERFORM etl_ops.recover_transformation_logs();
                END IF;
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
                    IF receiver_result->'transformation_run'->>'status' = 'FAIL' THEN
                        RAISE EXCEPTION 'Scheduled transformation rejected; inspect etl_ops.transformation_logs';
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
        sql.Identifier(OPS_SCHEMA, 'run_scheduled_extraction'),
        sql.Literal(json.dumps(SOURCE_COLUMNS)),
        sql.SQL('\n').join(stages),
        check_statement('*', 'source_connection', "'PASS'"),
        sql.Literal(EXTRACTION_LOCK_KEY[0]), sql.Literal(EXTRACTION_LOCK_KEY[1]),
        check_statement('*', 'overlap_lock', "'PASS'"),
        check_statement('*', 'snapshot_consistency', "'PASS'"),
        sql.Literal(SOURCE_SCHEMA),
        check_statement('*', 'schema_compatibility', "'PASS'"),
        sql.Literal(SOURCE_SCHEMA),
        check_statement('*', 'source_accessibility', "'PASS'"),
        sql.SQL('\n').join(reads), sql.SQL('\n').join(validations),
        check_statement('*', 'completion', "'PASS'"),
        sql.Identifier(OPS_SCHEMA, 'extraction_handover'),
        check_statement('*', 'handover', "'PASS'"),
        sql.Literal(SOURCE_SCHEMA),
        sql.Identifier(OPS_SCHEMA, 'run_scheduled_extraction'),
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


def run_scheduled_once(connection) -> int:
    """`python -m pipeline.scheduling run`: run the Cron command once, then summarize it.

    The summary is built from the run's extraction, transformation, and load
    logs (Load.md Section 8). A stage that stops the run raises an exception in
    the procedure; its message is shown when the logs do not explain it.
    """
    # Procedures that commit cannot be called inside a client transaction.
    connection.autocommit = True
    message = None
    with connection.cursor() as cursor:
        cursor.execute('SELECT clock_timestamp()')
        started = cursor.fetchone()[0]
        with report.Progress('Running extraction, transformation, and load in the database') as progress:
            try:
                cursor.execute(JOB_COMMAND)
            except psycopg.Error as error:
                if connection.broken:
                    raise
                message = error.diag.message_primary or f'SQLSTATE {error.sqlstate}'
            progress.update('Preparing the summary')
            extraction_result, stage2, load_entry, last_published = report.fetch_scheduled_run(
                cursor, started)
    print(report.render(started, extraction_result, stage2, load_entry,
                        last_published=last_published, database_message=message,
                        details_hint='python -m pipeline.scheduling diagnose'))
    return report.exit_code(extraction_result, stage2, load_entry)


def manage_schedule(action):
    if action == 'setup':
        setup_extraction_logs()
    with connect_source() as connection:
        if action == 'update-handover':
            connection.read_only = False
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT consume_function::text
                    FROM etl_ops.extraction_handover
                    WHERE singleton
                ''')
                receiver = cursor.fetchone()
                if not receiver or receiver[0] != 'etl_ops.consume_transformation_with_load(jsonb)':
                    raise ValueError('The configured extraction receiver changed; inspect it before updating.')
                cursor.execute(load_handover_consumer_sql())
            print('Stage 2-to-Load handover updated with the required batch_window.')
            return
        if action == 'inspect-handover':
            # The scheduled consumer is installed in PostgreSQL.  Show the
            # active definition so a contract fix targets the live function.
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT consume_function::text,
                           pg_get_functiondef(consume_function::oid)
                    FROM etl_ops.extraction_handover
                    WHERE singleton
                ''')
                row = cursor.fetchone()
            if not row:
                print('No extraction handover receiver is configured.')
                return
            print(f'Configured consumer: {row[0]}')
            print(row[1])
            return
        if action == 'diagnose':
            # Keep this query read-only.  The error message identifies the
            # failed handover contract field without re-running the pipeline.
            with connection.cursor() as cursor:
                cursor.execute('''
                    SELECT load_ended_at, load_run_id, transformation_run_id,
                           extraction_run_id, input_acceptance_status,
                           error_code, error_message, pre_load_results
                    FROM etl_ops.load_logs
                    ORDER BY load_ended_at DESC
                    LIMIT 1
                ''')
                row = cursor.fetchone()
            if not row:
                print('No load-log entries were found.')
                return
            labels = ('ended_at', 'load_run_id', 'transformation_run_id',
                      'extraction_run_id', 'input_acceptance_status',
                      'error_code', 'error_message', 'pre_load_results')
            print(json.dumps(dict(zip(labels, row)), default=str, indent=2))
            return
        connection.read_only = False
        if action == 'run':
            return run_scheduled_once(connection)
        if action == 'update':
            with connection.cursor() as cursor:
                cursor.execute(log_recovery_sql())
                cursor.execute(handover_config_sql())
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
                cursor.execute(handover_config_sql())
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
    if len(sys.argv) != 2 or sys.argv[1] not in ('setup','run','enable','disable','status','update','recover','diagnose','inspect-handover','update-handover'):
        print('Usage: python -m pipeline.scheduling setup|run|enable|disable|status|update|recover|diagnose|inspect-handover|update-handover')
        return 1
    try:
        result = manage_schedule(sys.argv[1])
    except ValueError as error:
        print(error)
        return 1
    except psycopg.Error as error:
        print(f'Scheduling operation failed (SQLSTATE: {error.sqlstate or "unavailable"}).')
        # PostgreSQL uses P0001 for an intentional RAISE EXCEPTION.  The
        # scheduled procedures use that state when a pipeline stage rejects a
        # run, so surface the server's diagnostic without exposing credentials.
        if error.diag.message_primary:
            print(error.diag.message_primary)
        if error.diag.context:
            print(error.diag.context)
        print('Check permissions, pg_cron, and database logs.')
        return 1
    except SourceConnectionError:
        print('Scheduling operation failed. Check permissions, pg_cron, and database logs.')
        return 1
    return result if isinstance(result, int) else 0


if __name__ == '__main__':
    raise SystemExit(main())
