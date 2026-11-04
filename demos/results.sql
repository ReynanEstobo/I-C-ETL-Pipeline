-- Latest demo extraction, including its transformation if extraction approved it.
WITH latest AS (
    SELECT pipeline_run_id FROM etl_ops.extraction_logs
    WHERE table_name LIKE 'etl_demo.%'
    ORDER BY extraction_started_at DESC LIMIT 1
)
SELECT e.pipeline_run_id, e.table_name, e.extraction_status,
       e.acceptance_status, e.records_read, e.records_extracted,
       e.validation_results, e.error_code,
       t.metadata->>'status' AS transformation_status,
       t.metadata->>'acceptance_status' AS transformation_acceptance,
       t.metadata->>'records_output' AS output_groups,
       t.metadata->'issues' AS transformation_issues
FROM latest l JOIN etl_ops.extraction_logs e USING (pipeline_run_id)
LEFT JOIN etl_ops.transformation_logs t ON t.parent_run_id=e.pipeline_run_id
ORDER BY e.table_name;
