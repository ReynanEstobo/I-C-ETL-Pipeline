-- Enables row-level tolerance for the installed Stage 2 transformation.
-- Run once in Supabase SQL Editor as the ETL owner, after the original
-- transformation installation. It preserves the original function as
-- etl_ops.consume_transformation_strict(jsonb) for auditability.
BEGIN;

DO $$
BEGIN
    IF to_regprocedure('etl_ops.consume_transformation_strict(jsonb)') IS NULL THEN
        IF to_regprocedure('etl_ops.consume_transformation(jsonb)') IS NULL THEN
            RAISE EXCEPTION 'Install the Stage 2 transformation before enabling tolerance';
        END IF;
        ALTER FUNCTION etl_ops.consume_transformation(jsonb)
            RENAME TO consume_transformation_strict;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION etl_ops.consume_transformation(payload jsonb)
RETURNS jsonb
LANGUAGE plpgsql SECURITY INVOKER AS $tolerant$
DECLARE
    strict_response jsonb;
    result jsonb;
    cutoff date;
    can_quarantine boolean;
    output_count bigint;
    quarantined_count bigint;
BEGIN
    -- The strict implementation still identifies every issue and preserves its
    -- existing validations. This wrapper only permits independently attributable
    -- fact rows to be set aside after that decision is available.
    -- The original function only accepts APPROVED. Its caller may send the
    -- stronger provenance label APPROVED_WITH_WARNINGS after extraction found
    -- nullable source fields; preserve the original payload in returned lineage.
    strict_response := etl_ops.consume_transformation_strict(
        payload || jsonb_build_object('acceptance_status', 'APPROVED')
    );
    result := strict_response->'transformation_run';
    cutoff := (payload->>'extraction_started_at')::timestamptz AT TIME ZONE 'Asia/Manila';

    SELECT count(*) > 0
       AND bool_and(i.record_id IS NOT NULL
                    AND i.table_name IN ('orders', 'payments', 'expenses'))
    INTO can_quarantine
    FROM pg_temp.transformation_issues i;

    -- Never tolerate a database/function error, a dimension issue, an output
    -- reconciliation issue, or a batch that has no attributable fact-row issue.
    IF result->>'status' NOT IN ('FAIL', 'WARNING') OR NOT coalesce(can_quarantine, false) THEN
        RETURN strict_response;
    END IF;

    -- Reclassify every affected fact row as quarantined/rejected. Valid rows are
    -- then the only rows eligible for the new temporary output.
    UPDATE pg_temp.transformation_accounting a
    SET records_rejected = (
            SELECT count(*) FROM pg_temp.stage_orders s
            WHERE EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                          WHERE i.table_name='orders' AND i.record_id=s.id)
        ), records_excluded = (
            SELECT count(*) FROM pg_temp.stage_orders s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='orders' AND i.record_id=s.id)
              AND lower(btrim(s.status))='cancelled'
        ), records_deferred = (
            SELECT count(*) FROM pg_temp.stage_orders s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='orders' AND i.record_id=s.id)
              AND lower(btrim(s.status))<>'cancelled'
              AND (s.created_at AT TIME ZONE 'Asia/Manila')::date >= cutoff
        ), records_contributing = (
            SELECT count(*) FROM pg_temp.stage_orders s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='orders' AND i.record_id=s.id)
              AND lower(btrim(s.status))<>'cancelled'
              AND (s.created_at AT TIME ZONE 'Asia/Manila')::date < cutoff
        )
    WHERE a.table_name='orders';

    UPDATE pg_temp.transformation_accounting a
    SET records_rejected = (
            SELECT count(*) FROM pg_temp.stage_payments s
            WHERE EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                          WHERE i.table_name='payments' AND i.record_id=s.id)
        ), records_excluded = 0, records_deferred = (
            SELECT count(*) FROM pg_temp.stage_payments s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='payments' AND i.record_id=s.id)
              AND (s.paid_at AT TIME ZONE 'Asia/Manila')::date >= cutoff
        ), records_contributing = (
            SELECT count(*) FROM pg_temp.stage_payments s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='payments' AND i.record_id=s.id)
              AND (s.paid_at AT TIME ZONE 'Asia/Manila')::date < cutoff
        )
    WHERE a.table_name='payments';

    UPDATE pg_temp.transformation_accounting a
    SET records_rejected = (
            SELECT count(*) FROM pg_temp.stage_expenses s
            WHERE EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                          WHERE i.table_name='expenses' AND i.record_id=s.id)
        ), records_excluded = (
            SELECT count(*) FROM pg_temp.stage_expenses s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='expenses' AND i.record_id=s.id)
              AND s.deleted_at IS NOT NULL
        ), records_deferred = (
            SELECT count(*) FROM pg_temp.stage_expenses s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='expenses' AND i.record_id=s.id)
              AND s.deleted_at IS NULL AND s.expense_date >= cutoff
        ), records_contributing = (
            SELECT count(*) FROM pg_temp.stage_expenses s
            WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                              WHERE i.table_name='expenses' AND i.record_id=s.id)
              AND s.deleted_at IS NULL AND s.expense_date < cutoff
        )
    WHERE a.table_name='expenses';

    DELETE FROM pg_temp.transformed_daily_branch_summary;
    INSERT INTO pg_temp.transformed_daily_branch_summary
    WITH o AS (
        SELECT (s.created_at AT TIME ZONE 'Asia/Manila')::date d, s.branch_id b,
            count(*) oc, count(*) FILTER (WHERE lower(btrim(s.status))='released') rc,
            count(DISTINCT s.customer_id) uc, round(sum(s.weight_kg), 2) w,
            round(sum(s.total_price), 2) v
        FROM pg_temp.stage_orders s
        WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                          WHERE i.table_name='orders' AND i.record_id=s.id)
          AND lower(btrim(s.status))<>'cancelled'
          AND (s.created_at AT TIME ZONE 'Asia/Manila')::date < cutoff
        GROUP BY 1, 2
    ), p AS (
        SELECT (s.paid_at AT TIME ZONE 'Asia/Manila')::date d, s.branch_id b,
            count(*) pc, round(sum(s.amount), 2) cash
        FROM pg_temp.stage_payments s
        WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                          WHERE i.table_name='payments' AND i.record_id=s.id)
          AND (s.paid_at AT TIME ZONE 'Asia/Manila')::date < cutoff
        GROUP BY 1, 2
    ), e AS (
        SELECT s.expense_date d, s.branch_id b, round(sum(s.amount), 2) expense
        FROM pg_temp.stage_expenses s
        WHERE NOT EXISTS (SELECT 1 FROM pg_temp.transformation_issues i
                          WHERE i.table_name='expenses' AND i.record_id=s.id)
          AND s.deleted_at IS NULL AND s.expense_date < cutoff
        GROUP BY 1, 2
    ), keys AS (SELECT d,b FROM o UNION SELECT d,b FROM p UNION SELECT d,b FROM e)
    SELECT k.d, k.b, coalesce(oc,0), coalesce(rc,0), coalesce(uc,0),
           coalesce(w,0), coalesce(v,0), coalesce(pc,0), coalesce(cash,0),
           coalesce(expense,0)
    FROM keys k LEFT JOIN o USING(d,b) LEFT JOIN p USING(d,b) LEFT JOIN e USING(d,b);

    SELECT count(*) INTO output_count FROM pg_temp.transformed_daily_branch_summary;
    SELECT count(DISTINCT (table_name, record_id)) INTO quarantined_count
    FROM pg_temp.transformation_issues
    WHERE table_name IN ('orders','payments','expenses') AND record_id IS NOT NULL;

    IF output_count = 0 THEN
        RETURN strict_response;
    END IF;

    result := result || jsonb_build_object(
        'status', 'PARTIAL',
        'acceptance_status', 'APPROVED_WITH_QUARANTINE',
        'rule_version', '1.1.0',
        'records_output', output_count,
        'quarantined_records', quarantined_count,
        'tolerance_policy', 'fact_rows_quarantined_valid_rows_contributed',
        'validation_results', jsonb_build_object(
            'status', 'PARTIAL', 'issues', coalesce(result->'issues','[]'::jsonb)
        )
    );
    RETURN jsonb_build_object('transformation_run', result);
END;
$tolerant$;
REVOKE ALL ON FUNCTION etl_ops.consume_transformation(jsonb) FROM PUBLIC;

-- Keep database-delivered warnings and failures as minimal quarantine evidence.
CREATE OR REPLACE FUNCTION etl_ops.recover_transformation_logs() RETURNS bigint
LANGUAGE plpgsql SECURITY INVOKER AS $recovery$
DECLARE item record; issue record; n bigint:=0;
BEGIN
    PERFORM pg_advisory_xact_lock(18755,1163152462);
    FOR item IN SELECT * FROM etl_ops.pending_transformation_logs FOR UPDATE LOOP
        INSERT INTO etl_ops.transformation_logs VALUES (item.run_id,
            (item.metadata->>'parent_run_id')::uuid,(item.metadata->>'ended_at')::timestamptz,
            item.metadata || jsonb_build_object('log_delivery_status','SAVED')) ON CONFLICT DO NOTHING;
        FOR issue IN SELECT value, ordinality
            FROM jsonb_array_elements(coalesce(item.metadata->'issues','[]'::jsonb)) WITH ORDINALITY LOOP
            IF issue.value->>'severity' IN ('FAIL','WARNING')
               AND issue.value->>'record_id' IS NOT NULL THEN
                INSERT INTO etl_ops.transformation_quarantine VALUES (item.run_id, issue.ordinality-1,
                    (item.metadata->>'ended_at')::timestamptz, issue.value) ON CONFLICT DO NOTHING;
            END IF;
        END LOOP;
        DELETE FROM etl_ops.pending_transformation_logs WHERE run_id=item.run_id;
        n:=n+1;
    END LOOP;
    RETURN n;
END;
$recovery$;
REVOKE ALL ON FUNCTION etl_ops.recover_transformation_logs() FROM PUBLIC;

-- The scheduled extraction handover stores a regprocedure OID. Point it at the
-- new wrapper after the original function was renamed.
UPDATE etl_ops.extraction_handover
SET consume_function='etl_ops.consume_transformation(jsonb)'::regprocedure;

-- Refresh the optional isolated presentation demo, if it was installed.
DO $$
BEGIN
    IF to_regclass('etl_demo.extraction_handover') IS NOT NULL THEN
        UPDATE etl_demo.extraction_handover
        SET consume_function='etl_ops.consume_transformation(jsonb)'::regprocedure;
    END IF;
END $$;

COMMIT;
