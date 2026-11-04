-- OPTIONAL invalid-record demonstrations against REAL public source tables.
-- Do NOT run all cases together. Each case affects your normal daily ETL until
-- you run its matching cleanup statement. Run a single CASE, run the pipeline,
-- inspect etl_ops logs, then run its CLEANUP.
--
-- Your database constraints stop invalid payment amounts, duplicate primary keys,
-- and unknown statuses before ETL can read them. These three cases are invalid
-- according to your ETL rules but permitted by your current public table schema.

-- CASE 1: Extraction warning. Missing order branch means MANUAL REVIEW.
-- Expected: extraction WARNING / MANUAL REVIEW; transformation is not called.
/*
INSERT INTO public.orders (
    id, order_number, customer_id, branch_id, branch, weight_kg, total_price,
    status, payment_status, payment_method, created_at
) VALUES (
    'f1000000-0000-0000-0000-000000000001', 'INVALID-MISSING-BRANCH',
    'b1000000-0000-0000-0000-000000000001', NULL, NULL, 8, 200,
    'released', 'unpaid', NULL, '2026-10-23 02:00:00+00'
) ON CONFLICT DO NOTHING;
*/
-- CASE 1 CLEANUP (run after logging the result):
-- DELETE FROM public.orders WHERE id='f1000000-0000-0000-0000-000000000001';

-- CASE 2: Transformation rejection. Negative weight passes extraction but fails
-- transformation rule C-04. Run only after CASE 1 has been cleaned up.
/*
INSERT INTO public.orders (
    id, order_number, customer_id, branch_id, branch, weight_kg, total_price,
    status, payment_status, payment_method, created_at
) VALUES (
    'f1000000-0000-0000-0000-000000000002', 'INVALID-NEGATIVE-WEIGHT',
    'b1000000-0000-0000-0000-000000000001',
    'a1000000-0000-0000-0000-000000000001', 'Downtown Branch', -2, 200,
    'released', 'unpaid', NULL, '2026-10-23 02:00:00+00'
) ON CONFLICT DO NOTHING;
*/
-- CASE 2 CLEANUP (run after logging the result):
-- DELETE FROM public.orders WHERE id='f1000000-0000-0000-0000-000000000002';

-- CASE 3: Extraction warning. The source schema permits a NULL created_at.
-- Expected: extraction WARNING / MANUAL REVIEW; transformation is not called.
/*
INSERT INTO public.orders (
    id, order_number, customer_id, branch_id, branch, weight_kg, total_price,
    status, payment_status, payment_method, created_at
) VALUES (
    'f1000000-0000-0000-0000-000000000003', 'INVALID-MISSING-CREATED-AT',
    'b1000000-0000-0000-0000-000000000001',
    'a1000000-0000-0000-0000-000000000001', 'Downtown Branch', 8, 200,
    'released', 'unpaid', NULL, NULL
) ON CONFLICT DO NOTHING;
*/
-- CASE 3 CLEANUP (run after logging the result):
-- DELETE FROM public.orders WHERE id='f1000000-0000-0000-0000-000000000003';

-- Check whether an optional invalid record is still present before a normal run:
SELECT id, order_number, branch_id, created_at, weight_kg
FROM public.orders
WHERE id::text LIKE 'f1000000%'
ORDER BY id;
