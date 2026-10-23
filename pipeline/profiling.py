"""Profile an approved extraction without changing source records."""

import json

from pipeline.extraction import main as extract_main


class ProfileConsumer:
    def acknowledge(self, metadata):
        return metadata

    def consume(self, connection, payload):
        queries = {
            'statuses': 'SELECT status, count(*) FROM pg_temp.stage_orders GROUP BY status ORDER BY status',
            'labels': "SELECT count(*) FILTER (WHERE branch_name IS NULL OR btrim(branch_name) = ''), count(*) FILTER (WHERE branch_name <> btrim(branch_name)), count(*) FILTER (WHERE branch_name ~ '[[:cntrl:]]') FROM pg_temp.stage_branches",
            'order_numbers': "SELECT count(*) FILTER (WHERE weight_kg < 0), count(*) FILTER (WHERE total_price < 0), count(*) FILTER (WHERE weight_kg IS NULL OR total_price IS NULL), count(*) FILTER (WHERE weight_kg::text IN ('NaN','Infinity','-Infinity') OR total_price::text IN ('NaN','Infinity','-Infinity')) FROM pg_temp.stage_orders",
            'payment_amounts': "SELECT count(*) FILTER (WHERE amount < 0), count(*) FILTER (WHERE amount IS NULL), count(*) FILTER (WHERE amount::text IN ('NaN','Infinity','-Infinity')) FROM pg_temp.stage_payments",
            'expense_amounts': "SELECT count(*) FILTER (WHERE amount < 0), count(*) FILTER (WHERE amount IS NULL), count(*) FILTER (WHERE amount::text IN ('NaN','Infinity','-Infinity')), count(*) FILTER (WHERE deleted_at IS NOT NULL) FROM pg_temp.stage_expenses",
            'cross_branch': 'SELECT count(*) FROM pg_temp.stage_payments p JOIN pg_temp.stage_orders o ON o.id=p.order_id WHERE p.branch_id IS DISTINCT FROM o.branch_id',
            'payment_multiplicity': 'SELECT payment_count, count(*) FROM (SELECT order_id, count(*) AS payment_count FROM pg_temp.stage_payments GROUP BY order_id) x GROUP BY payment_count ORDER BY payment_count',
        }
        result = {
            'extraction_run_id': str(payload['pipeline_run_id']),
            'started_at': payload['extraction_started_at'].isoformat(),
            'counts': payload['records_extracted'],
            'profiles': {name: connection.execute(query).fetchall() for name, query in queries.items()},
        }
        print(json.dumps(result, default=str, indent=2))


if __name__ == '__main__':
    raise SystemExit(extract_main(ProfileConsumer()))
