-- Generated from pipeline.scheduling.procedure_sql for isolated demo sources.
-- Run once as ETL owner. Requires the existing etl_ops installation.
BEGIN;
CREATE SCHEMA etl_demo;
REVOKE ALL ON SCHEMA etl_demo FROM PUBLIC;
CREATE TABLE etl_demo."branches" ("id" uuid, "branch_name" text, "name" text, "code" text);
REVOKE ALL ON etl_demo."branches" FROM PUBLIC;
CREATE TABLE etl_demo."customers" ("id" uuid);
REVOKE ALL ON etl_demo."customers" FROM PUBLIC;
CREATE TABLE etl_demo."orders" ("id" uuid, "order_number" text, "branch_id" uuid, "customer_id" uuid, "created_at" timestamptz, "status" text, "weight_kg" numeric, "total_price" numeric);
REVOKE ALL ON etl_demo."orders" FROM PUBLIC;
CREATE TABLE etl_demo."payments" ("id" uuid, "order_id" uuid, "branch_id" uuid, "amount" numeric, "paid_at" timestamptz, "source" text);
REVOKE ALL ON etl_demo."payments" FROM PUBLIC;
CREATE TABLE etl_demo."expenses" ("id" uuid, "branch_id" uuid, "amount" numeric, "expense_date" date, "deleted_at" timestamptz);
REVOKE ALL ON etl_demo."expenses" FROM PUBLIC;

        CREATE TABLE IF NOT EXISTS "etl_demo"."extraction_handover" (
            singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
            acknowledge_function regprocedure NOT NULL,
            consume_function regprocedure NOT NULL
        );
        REVOKE ALL ON "etl_demo"."extraction_handover" FROM PUBLIC;
        ALTER TABLE "etl_demo"."extraction_handover" ENABLE ROW LEVEL SECURITY;
    

        CREATE OR REPLACE PROCEDURE "etl_demo"."run_scheduled_extraction"()
        LANGUAGE plpgsql SECURITY INVOKER AS $procedure$
        DECLARE
            run_id uuid;
            started timestamptz;
            ended timestamptz;
            checks jsonb;
            source_counts jsonb;
            staged_counts jsonb;
            expected jsonb := '{"branches": {"id": "uuid", "branch_name": "text", "name": "text", "code": "text"}, "customers": {"id": "uuid"}, "orders": {"id": "uuid", "order_number": "text", "branch_id": "uuid", "customer_id": "uuid", "created_at": "timestamptz", "status": "text", "weight_kg": "numeric", "total_price": "numeric"}, "payments": {"id": "uuid", "order_id": "uuid", "branch_id": "uuid", "amount": "numeric", "paid_at": "timestamptz", "source": "text"}, "expenses": {"id": "uuid", "branch_id": "uuid", "amount": "numeric", "expense_date": "date", "deleted_at": "timestamptz"}}'::jsonb;
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
-- Demo-only reset permits repeated calls in the same SQL session.
DROP TABLE IF EXISTS pg_temp.stage_branches;
DROP TABLE IF EXISTS pg_temp.stage_customers;
DROP TABLE IF EXISTS pg_temp.stage_orders;
DROP TABLE IF EXISTS pg_temp.stage_payments;
DROP TABLE IF EXISTS pg_temp.stage_expenses;
DROP TABLE IF EXISTS pg_temp.transformation_issues;
DROP TABLE IF EXISTS pg_temp.transformation_accounting;
DROP TABLE IF EXISTS pg_temp.transformation_rules;
DROP TABLE IF EXISTS pg_temp.transformed_daily_branch_summary;
            PERFORM etl_ops.recover_extraction_logs();
            CREATE TEMP TABLE "stage_branches" ("id" uuid, "branch_name" text, "name" text, "code" text) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE "stage_customers" ("id" uuid) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE "stage_orders" ("id" uuid, "order_number" text, "branch_id" uuid, "customer_id" uuid, "created_at" timestamptz, "status" text, "weight_kg" numeric, "total_price" numeric) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE "stage_payments" ("id" uuid, "order_id" uuid, "branch_id" uuid, "amount" numeric, "paid_at" timestamptz, "source" text) ON COMMIT DELETE ROWS;
CREATE TEMP TABLE "stage_expenses" ("id" uuid, "branch_id" uuid, "amount" numeric, "expense_date" date, "deleted_at" timestamptz) ON COMMIT DELETE ROWS;

        IF to_regprocedure('etl_ops.prepare_transformation()') IS NOT NULL THEN
            PERFORM etl_ops.recover_transformation_logs();
            PERFORM etl_ops.prepare_transformation();
        END IF;
    
            SET SESSION CHARACTERISTICS AS TRANSACTION
                ISOLATION LEVEL REPEATABLE READ, READ ONLY;
            COMMIT;

            FOR attempt IN 1..3 LOOP
                run_id := gen_random_uuid();
                started := clock_timestamp();
                checks := '[]'::jsonb;
                source_counts := '{}'::jsonb;
                staged_counts := '{}'::jsonb;
                failure_code := NULL;
                failure_message := NULL;
                receiver_result := NULL;
                failure_state := NULL;
                may_retry := false;
                overall := 'PASS';
                handover_status := 'NOT_ATTEMPTED';
                
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', '*', 'check', 'source_connection', 'status', 'PASS', 'affected_rows', 0));
    

                -- The exception subtransaction rolls back staged writes but
                -- retains local metadata for the following logging transaction.
                BEGIN
                    phase := 'overlap_lock';
                    IF NOT pg_try_advisory_xact_lock(18755, 1163152460) THEN
                        failure_code := 'EXTRACTION_ALREADY_RUNNING';
                        RAISE EXCEPTION 'Extraction already running';
                    END IF;
                    
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', '*', 'check', 'overlap_lock', 'status', 'PASS', 'affected_rows', 0));
    
                    phase := 'snapshot_consistency';
                    IF current_setting('transaction_read_only') <> 'on'
                       OR current_setting('transaction_isolation') <> 'repeatable read' THEN
                        RAISE EXCEPTION 'Invalid extraction transaction settings';
                    END IF;
                    
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', '*', 'check', 'snapshot_consistency', 'status', 'PASS', 'affected_rows', 0));
    

                    phase := 'schema_compatibility';
                    FOR table_entry IN SELECT * FROM jsonb_each(expected) LOOP
                        FOR column_entry IN SELECT * FROM jsonb_each_text(table_entry.value) LOOP
                            SELECT t.typname INTO actual_type
                            FROM pg_catalog.pg_class c
                            JOIN pg_catalog.pg_namespace ns ON ns.oid = c.relnamespace
                            JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
                            JOIN pg_catalog.pg_type t ON t.oid = a.atttypid
                            WHERE ns.nspname = 'etl_demo' AND c.relname = table_entry.key
                              AND a.attname = column_entry.key
                              AND a.attnum > 0 AND NOT a.attisdropped
                              AND c.relkind IN ('r', 'p');
                            IF actual_type IS DISTINCT FROM column_entry.value THEN
                                failure_code := 'SCHEMA_MISMATCH';
                                RAISE EXCEPTION 'Source schema does not match extraction contract';
                            END IF;
                        END LOOP;
                    END LOOP;
                    
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', '*', 'check', 'schema_compatibility', 'status', 'PASS', 'affected_rows', 0));
    
                    phase := 'source_accessibility';
                    FOR table_entry IN SELECT * FROM jsonb_each(expected) LOOP
                        FOR column_entry IN SELECT * FROM jsonb_each_text(table_entry.value) LOOP
                            EXECUTE format('SELECT %I FROM %I.%I LIMIT 0',
                                           column_entry.key, 'etl_demo', table_entry.key);
                        END LOOP;
                    END LOOP;
                    
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', '*', 'check', 'source_accessibility', 'status', 'PASS', 'affected_rows', 0));
    
                    phase := 'completion';
                    
            SELECT count(*) INTO n FROM "etl_demo"."branches";
            source_counts := source_counts || jsonb_build_object('branches', n);
            INSERT INTO "pg_temp"."stage_branches" ("id", "branch_name", "name", "code") SELECT "id", "branch_name", "name", "code" FROM "etl_demo"."branches";
            SELECT count(*) INTO n FROM "pg_temp"."stage_branches";
            staged_counts := staged_counts || jsonb_build_object('branches', n);
        

            SELECT count(*) INTO n FROM "etl_demo"."customers";
            source_counts := source_counts || jsonb_build_object('customers', n);
            INSERT INTO "pg_temp"."stage_customers" ("id") SELECT "id" FROM "etl_demo"."customers";
            SELECT count(*) INTO n FROM "pg_temp"."stage_customers";
            staged_counts := staged_counts || jsonb_build_object('customers', n);
        

            SELECT count(*) INTO n FROM "etl_demo"."orders";
            source_counts := source_counts || jsonb_build_object('orders', n);
            INSERT INTO "pg_temp"."stage_orders" ("id", "order_number", "branch_id", "customer_id", "created_at", "status", "weight_kg", "total_price") SELECT "id", "order_number", "branch_id", "customer_id", "created_at", "status", "weight_kg", "total_price" FROM "etl_demo"."orders";
            SELECT count(*) INTO n FROM "pg_temp"."stage_orders";
            staged_counts := staged_counts || jsonb_build_object('orders', n);
        

            SELECT count(*) INTO n FROM "etl_demo"."payments";
            source_counts := source_counts || jsonb_build_object('payments', n);
            INSERT INTO "pg_temp"."stage_payments" ("id", "order_id", "branch_id", "amount", "paid_at", "source") SELECT "id", "order_id", "branch_id", "amount", "paid_at", "source" FROM "etl_demo"."payments";
            SELECT count(*) INTO n FROM "pg_temp"."stage_payments";
            staged_counts := staged_counts || jsonb_build_object('payments', n);
        

            SELECT count(*) INTO n FROM "etl_demo"."expenses";
            source_counts := source_counts || jsonb_build_object('expenses', n);
            INSERT INTO "pg_temp"."stage_expenses" ("id", "branch_id", "amount", "expense_date", "deleted_at") SELECT "id", "branch_id", "amount", "expense_date", "deleted_at" FROM "etl_demo"."expenses";
            SELECT count(*) INTO n FROM "pg_temp"."stage_expenses";
            staged_counts := staged_counts || jsonb_build_object('expenses', n);
        
                    SELECT abs((SELECT count(*) FROM "etl_demo"."branches") - (SELECT count(*) FROM "pg_temp"."stage_branches")) INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'branches', 'check', 'row_count', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_branches" WHERE "id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'branches', 'check', 'id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count("id") - count(DISTINCT "id") FROM "pg_temp"."stage_branches" INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'branches', 'check', 'id_unique', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT abs((SELECT count(*) FROM "etl_demo"."customers") - (SELECT count(*) FROM "pg_temp"."stage_customers")) INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'customers', 'check', 'row_count', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_customers" WHERE "id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'customers', 'check', 'id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count("id") - count(DISTINCT "id") FROM "pg_temp"."stage_customers" INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'customers', 'check', 'id_unique', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT abs((SELECT count(*) FROM "etl_demo"."orders") - (SELECT count(*) FROM "pg_temp"."stage_orders")) INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'row_count', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" WHERE "id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count("id") - count(DISTINCT "id") FROM "pg_temp"."stage_orders" INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'id_unique', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" WHERE "order_number" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'order_number_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count("order_number") - count(DISTINCT "order_number") FROM "pg_temp"."stage_orders" INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'order_number_unique', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT abs((SELECT count(*) FROM "etl_demo"."payments") - (SELECT count(*) FROM "pg_temp"."stage_payments")) INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'row_count', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_payments" WHERE "id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count("id") - count(DISTINCT "id") FROM "pg_temp"."stage_payments" INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'id_unique', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT abs((SELECT count(*) FROM "etl_demo"."expenses") - (SELECT count(*) FROM "pg_temp"."stage_expenses")) INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'expenses', 'check', 'row_count', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_expenses" WHERE "id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'expenses', 'check', 'id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count("id") - count(DISTINCT "id") FROM "pg_temp"."stage_expenses" INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'expenses', 'check', 'id_unique', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" WHERE "branch_id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'branch_id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'WARNING' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" AS child WHERE child."branch_id" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "pg_temp"."stage_branches" AS parent WHERE parent.id = child."branch_id") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'branch_id_reference', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" WHERE "customer_id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'customer_id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'WARNING' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" AS child WHERE child."customer_id" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "pg_temp"."stage_customers" AS parent WHERE parent.id = child."customer_id") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'customer_id_reference', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_payments" WHERE "branch_id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'branch_id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'WARNING' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_payments" AS child WHERE child."branch_id" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "pg_temp"."stage_branches" AS parent WHERE parent.id = child."branch_id") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'branch_id_reference', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_payments" WHERE "order_id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'order_id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_payments" AS child WHERE child."order_id" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "pg_temp"."stage_orders" AS parent WHERE parent.id = child."order_id") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'order_id_reference', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_expenses" WHERE "branch_id" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'expenses', 'check', 'branch_id_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'WARNING' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_expenses" AS child WHERE child."branch_id" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "pg_temp"."stage_branches" AS parent WHERE parent.id = child."branch_id") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'expenses', 'check', 'branch_id_reference', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" WHERE "created_at" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'created_at_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'WARNING' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_orders" WHERE "created_at" IS NOT NULL AND NOT isfinite("created_at") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'orders', 'check', 'created_at_valid', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_payments" WHERE "paid_at" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'paid_at_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_payments" WHERE "paid_at" IS NOT NULL AND NOT isfinite("paid_at") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'payments', 'check', 'paid_at_valid', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_expenses" WHERE "expense_date" IS NULL INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'expenses', 'check', 'expense_date_present', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
SELECT count(*) FROM "pg_temp"."stage_expenses" WHERE "expense_date" IS NOT NULL AND NOT isfinite("expense_date") INTO affected;

        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', 'expenses', 'check', 'expense_date_valid', 'status', CASE WHEN affected = 0 THEN 'PASS' ELSE 'FAIL' END, 'affected_rows', affected));
    
                    
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', '*', 'check', 'completion', 'status', 'PASS', 'affected_rows', 0));
    

                    SELECT CASE WHEN bool_or(c->>'status' = 'FAIL') THEN 'FAIL'
                                WHEN bool_or(c->>'status' = 'WARNING') THEN 'WARNING'
                                ELSE 'PASS' END
                    INTO overall FROM jsonb_array_elements(checks) c;
                    IF overall <> 'PASS' THEN
                        -- Roll back staged records without treating warnings
                        -- as execution failures or retrying data problems.
                        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Acceptance blocked';
                    END IF;
                    phase := 'handover';
                    SELECT h.acknowledge_function, h.consume_function
                    INTO acknowledge_function, consume_function FROM "etl_demo"."extraction_handover" h;
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
                            'pipeline_run_id', run_id, 'acceptance_status', 'APPROVED',
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
                                        'pipeline_run_id', run_id, 'acceptance_status', 'APPROVED');
                                IF acknowledgment->>'pipeline_run_id' IS DISTINCT FROM run_id::text
                                   OR acknowledgment->>'acceptance_status' IS DISTINCT FROM 'APPROVED' THEN
                                    RAISE EXCEPTION 'Next stage did not acknowledge the approved run';
                                END IF;
                            ELSE
                                EXECUTE format('SELECT %I.%I($1)', receiver_schema, receiver_name)
                                    INTO receiver_result USING handover_payload;
                            END IF;
                        END LOOP;
                        handover_status := 'ACKNOWLEDGED';
                        failure_code := NULL;
                        
        checks := checks || jsonb_build_array(jsonb_build_object(
            'table', '*', 'check', 'handover', 'status', 'PASS', 'affected_rows', 0));
    
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
                                          WHEN 'WARNING' THEN 'MANUAL REVIEW'
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
                        run_id, 'etl_demo' || '.' || table_entry.key, started, ended, table_status,
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
        REVOKE ALL ON PROCEDURE "etl_demo"."run_scheduled_extraction"() FROM PUBLIC;
    
INSERT INTO etl_demo.extraction_handover (acknowledge_function,consume_function) VALUES ('etl_ops.acknowledge_transformation(jsonb)'::regprocedure,'etl_ops.consume_transformation(jsonb)'::regprocedure);
COMMIT;