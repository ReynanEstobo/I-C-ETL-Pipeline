-- Run AFTER setup.sql, as the ETL owner. Changes ONLY etl_demo source tables.
-- Select ONE case, run it, then execute run.sql separately.
BEGIN;
CREATE FUNCTION etl_demo.seed(scenario text DEFAULT 'valid') RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    b uuid := '10000000-0000-0000-0000-000000000001';
    c uuid := '20000000-0000-0000-0000-000000000001';
    o uuid := '30000000-0000-0000-0000-000000000001';
BEGIN
    IF scenario NOT IN ('valid','missing_branch','duplicate_id','missing_date',
                        'negative_weight','unknown_status','zero_payment') THEN
        RAISE EXCEPTION 'Unknown demo scenario: %', scenario;
    END IF;
    TRUNCATE etl_demo.payments, etl_demo.expenses, etl_demo.orders,
             etl_demo.customers, etl_demo.branches;
    INSERT INTO etl_demo.branches VALUES (b,'Demo branch','Demo branch','DEMO');
    INSERT INTO etl_demo.customers VALUES (c);
    INSERT INTO etl_demo.orders VALUES
        (o,'DEMO-001',b,c,'2026-01-01 02:00:00+00','released',8,200);
    INSERT INTO etl_demo.payments VALUES
        ('40000000-0000-0000-0000-000000000001',o,b,100,'2026-01-01 02:00:00+00','demo'),
        ('40000000-0000-0000-0000-000000000002',o,b,100,'2026-01-01 03:00:00+00','demo');
    INSERT INTO etl_demo.expenses VALUES
        ('50000000-0000-0000-0000-000000000001',b,50,'2026-01-01',NULL),
        ('50000000-0000-0000-0000-000000000002',b,25,'2026-01-01','2026-01-02 00:00:00+00');

    CASE scenario
        WHEN 'missing_branch' THEN UPDATE etl_demo.orders SET branch_id=NULL;
        WHEN 'duplicate_id' THEN INSERT INTO etl_demo.customers SELECT * FROM etl_demo.customers;
        WHEN 'missing_date' THEN UPDATE etl_demo.payments SET paid_at=NULL;
        WHEN 'negative_weight' THEN UPDATE etl_demo.orders SET weight_kg=-1;
        WHEN 'unknown_status' THEN UPDATE etl_demo.orders SET status='unknown';
        WHEN 'zero_payment' THEN UPDATE etl_demo.payments SET amount=0;
        ELSE NULL;
    END CASE;
END $$;
REVOKE ALL ON FUNCTION etl_demo.seed(text) FROM PUBLIC;
SELECT etl_demo.seed('valid');
COMMIT;

-- Later, select and execute just ONE of these statements:
-- SELECT etl_demo.seed('valid');
-- SELECT etl_demo.seed('missing_branch');
-- SELECT etl_demo.seed('duplicate_id');
-- SELECT etl_demo.seed('missing_date');
-- SELECT etl_demo.seed('negative_weight');
-- SELECT etl_demo.seed('unknown_status');
-- SELECT etl_demo.seed('zero_payment');
