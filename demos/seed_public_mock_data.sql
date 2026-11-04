-- Optional presentation data for the REAL public tables.
-- Run in Supabase SQL Editor as the database owner.
-- This script only INSERTS records. It never deletes or updates existing data.
-- The dates are historical so the ETL transformation can include them in its
-- completed-day summary. Re-running the script is safe: duplicate keys are skipped.
-- Optional invalid ETL cases are deliberately separate in
-- demos/public_invalid_etl_cases.sql so this normal app dataset stays valid.

BEGIN;

-- Two branches visible in the application.
INSERT INTO public.branches (id, branch_name, name, code, address, status)
VALUES
    ('a1000000-0000-0000-0000-000000000001', 'Downtown Branch', 'Downtown Branch', 'DT01', '100 Main Street', 'active'),
    ('a1000000-0000-0000-0000-000000000002', 'Riverside Branch', 'Riverside Branch', 'RV01', '200 Riverside Road', 'active')
ON CONFLICT DO NOTHING;

-- Three valid customers. Phone numbers meet your ^09[0-9]{9}$ constraint.
INSERT INTO public.customers (id, name, phone, email, address, branch, branch_id)
VALUES
    ('b1000000-0000-0000-0000-000000000001', 'Ana Santos', '09171234567', 'ana.santos@example.test', 'Quezon City', 'Downtown Branch', 'a1000000-0000-0000-0000-000000000001'),
    ('b1000000-0000-0000-0000-000000000002', 'Marco Reyes', '09181234567', 'marco.reyes@example.test', 'Manila', 'Downtown Branch', 'a1000000-0000-0000-0000-000000000001'),
    ('b1000000-0000-0000-0000-000000000003', 'Liza Cruz', '09191234567', 'liza.cruz@example.test', 'Pasig', 'Riverside Branch', 'a1000000-0000-0000-0000-000000000002')
ON CONFLICT DO NOTHING;

-- Add 27 more customers for a presentation-sized dataset of 30 customers.
INSERT INTO public.customers (id, name, phone, email, address, branch, branch_id)
SELECT
    ('b1000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
    'Demo Customer ' || n,
    '0917' || lpad(n::text, 7, '0'),
    'customer' || n || '@example.test',
    'Demo Address ' || n,
    CASE WHEN n % 2 = 0 THEN 'Downtown Branch' ELSE 'Riverside Branch' END,
    CASE WHEN n % 2 = 0
         THEN 'a1000000-0000-0000-0000-000000000001'::uuid
         ELSE 'a1000000-0000-0000-0000-000000000002'::uuid END
FROM generate_series(4, 30) AS n
ON CONFLICT DO NOTHING;

-- Four orders: two released, one ready, and one cancelled.
INSERT INTO public.orders (
    id, order_number, customer_id, branch_id, branch, weight_kg, total_price,
    status, payment_status, payment_method, created_at
)
VALUES
    ('c1000000-0000-0000-0000-000000000001', 'DEMO-2026-001', 'b1000000-0000-0000-0000-000000000001', 'a1000000-0000-0000-0000-000000000001', 'Downtown Branch', 8.00, 200.00, 'released', 'paid', 'cash', '2026-10-20 02:00:00+00'),
    ('c1000000-0000-0000-0000-000000000002', 'DEMO-2026-002', 'b1000000-0000-0000-0000-000000000002', 'a1000000-0000-0000-0000-000000000001', 'Downtown Branch', 12.00, 300.00, 'ready', 'partial', 'gcash', '2026-10-21 03:00:00+00'),
    ('c1000000-0000-0000-0000-000000000003', 'DEMO-2026-003', 'b1000000-0000-0000-0000-000000000003', 'a1000000-0000-0000-0000-000000000002', 'Riverside Branch', 6.00, 180.00, 'released', 'paid', 'card', '2026-10-22 04:00:00+00'),
    ('c1000000-0000-0000-0000-000000000004', 'DEMO-2026-004', 'b1000000-0000-0000-0000-000000000001', 'a1000000-0000-0000-0000-000000000001', 'Downtown Branch', 5.00, 150.00, 'cancelled', 'unpaid', NULL, '2026-10-22 05:00:00+00')
ON CONFLICT DO NOTHING;

-- Add 26 more valid historical orders, giving 30 orders in total.
INSERT INTO public.orders (
    id, order_number, customer_id, branch_id, branch, weight_kg, total_price,
    status, payment_status, payment_method, created_at
)
SELECT
    ('c1000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
    'DEMO-2026-' || lpad(n::text, 3, '0'),
    ('b1000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
    CASE WHEN n % 2 = 0
         THEN 'a1000000-0000-0000-0000-000000000001'::uuid
         ELSE 'a1000000-0000-0000-0000-000000000002'::uuid END,
    CASE WHEN n % 2 = 0 THEN 'Downtown Branch' ELSE 'Riverside Branch' END,
    5 + (n % 8),
    (5 + (n % 8)) * 25,
    CASE WHEN n % 10 = 0 THEN 'cancelled'
         WHEN n % 3 = 0 THEN 'ready'
         ELSE 'released' END,
    CASE WHEN n % 10 = 0 THEN 'unpaid' ELSE 'paid' END,
    CASE WHEN n % 2 = 0 THEN 'cash' ELSE 'gcash' END,
    ('2026-10-01 02:00:00+00'::timestamptz + (n || ' days')::interval)
FROM generate_series(5, 30) AS n
ON CONFLICT DO NOTHING;

-- Multiple payments for order 002 demonstrate payment aggregation.
INSERT INTO public.payments (
    id, order_id, amount, payment_method, payment_status, paid_at, branch, branch_id, source
)
VALUES
    ('d1000000-0000-0000-0000-000000000001', 'c1000000-0000-0000-0000-000000000001', 200.00, 'cash', 'paid', '2026-10-20 04:00:00+00', 'Downtown Branch', 'a1000000-0000-0000-0000-000000000001', 'presentation_seed'),
    ('d1000000-0000-0000-0000-000000000002', 'c1000000-0000-0000-0000-000000000002', 100.00, 'gcash', 'partial', '2026-10-21 05:00:00+00', 'Downtown Branch', 'a1000000-0000-0000-0000-000000000001', 'presentation_seed'),
    ('d1000000-0000-0000-0000-000000000003', 'c1000000-0000-0000-0000-000000000002', 200.00, 'gcash', 'paid', '2026-10-21 06:00:00+00', 'Downtown Branch', 'a1000000-0000-0000-0000-000000000001', 'presentation_seed'),
    ('d1000000-0000-0000-0000-000000000004', 'c1000000-0000-0000-0000-000000000003', 180.00, 'card', 'paid', '2026-10-22 06:00:00+00', 'Riverside Branch', 'a1000000-0000-0000-0000-000000000002', 'presentation_seed')
ON CONFLICT DO NOTHING;

-- One payment for every additional order. Together with the four above: 30 payments.
INSERT INTO public.payments (
    id, order_id, amount, payment_method, payment_status, paid_at, branch, branch_id, source
)
SELECT
    ('d1000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
    ('c1000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
    (5 + (n % 8)) * 25,
    CASE WHEN n % 2 = 0 THEN 'cash' ELSE 'gcash' END,
    'paid',
    ('2026-10-01 04:00:00+00'::timestamptz + (n || ' days')::interval),
    CASE WHEN n % 2 = 0 THEN 'Downtown Branch' ELSE 'Riverside Branch' END,
    CASE WHEN n % 2 = 0
         THEN 'a1000000-0000-0000-0000-000000000001'::uuid
         ELSE 'a1000000-0000-0000-0000-000000000002'::uuid END,
    'presentation_seed'
FROM generate_series(5, 30) AS n
ON CONFLICT DO NOTHING;

-- One active expense and one soft-deleted expense. Transformation excludes deleted expenses.
INSERT INTO public.expenses (
    id, category, description, amount, expense_date, branch, branch_id, deleted_at
)
VALUES
    ('e1000000-0000-0000-0000-000000000001', 'Supplies', 'Detergent and packaging', 75.00, '2026-10-20', 'Downtown Branch', 'a1000000-0000-0000-0000-000000000001', NULL),
    ('e1000000-0000-0000-0000-000000000002', 'Utilities', 'Water expense', 120.00, '2026-10-21', 'Riverside Branch', 'a1000000-0000-0000-0000-000000000002', NULL),
    ('e1000000-0000-0000-0000-000000000003', 'Supplies', 'Voided test expense', 25.00, '2026-10-21', 'Downtown Branch', 'a1000000-0000-0000-0000-000000000001', '2026-10-22 00:00:00+00')
ON CONFLICT DO NOTHING;

-- Add 27 expenses for 30 total expenses; every seventh is soft-deleted.
INSERT INTO public.expenses (
    id, category, description, amount, expense_date, branch, branch_id, deleted_at
)
SELECT
    ('e1000000-0000-0000-0000-' || lpad(n::text, 12, '0'))::uuid,
    CASE WHEN n % 2 = 0 THEN 'Supplies' ELSE 'Utilities' END,
    'Presentation expense ' || n,
    40 + n,
    ('2026-10-01'::date + n),
    CASE WHEN n % 2 = 0 THEN 'Downtown Branch' ELSE 'Riverside Branch' END,
    CASE WHEN n % 2 = 0
         THEN 'a1000000-0000-0000-0000-000000000001'::uuid
         ELSE 'a1000000-0000-0000-0000-000000000002'::uuid END,
    CASE WHEN n % 7 = 0 THEN ('2026-10-31 00:00:00+00'::timestamptz) ELSE NULL END
FROM generate_series(4, 30) AS n
ON CONFLICT DO NOTHING;

COMMIT;

-- Optional verification, run after the INSERT statements:
-- SELECT 'branches' AS table_name, count(*) FROM public.branches WHERE id::text LIKE 'a1000000%'
-- UNION ALL SELECT 'customers', count(*) FROM public.customers WHERE id::text LIKE 'b1000000%'
-- UNION ALL SELECT 'orders', count(*) FROM public.orders WHERE order_number LIKE 'DEMO-2026-%'
-- UNION ALL SELECT 'payments', count(*) FROM public.payments WHERE source='presentation_seed'
-- UNION ALL SELECT 'expenses', count(*) FROM public.expenses WHERE id::text LIKE 'e1000000%';
