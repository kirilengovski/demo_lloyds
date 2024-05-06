-- Watermark to be passed as an argument
-- The watermark will be grabbed from the table where it lives in previous step of the DAG
-- The watermark represents the latest successful audit check
DECLARE watermark as $watermark

SELECT
    COALESCE(COUNT(raw.transaction_id), 0) / COUNT(enriched.transaction_id) * 100 AS matching_percentage
FROM
    raw_data_audit AS raw

-- Use LEFT join to include all records from the LEFT table so that we get the % of matching records
-- By joining on all important columns, we consider the data integrity in our % as well
LEFT JOIN
    enriched_data_audit AS enriched
ON
    raw.transaction_id = enriched.transaction_id
    AND raw.amount = enriched.amount
    AND raw.currency = enriched.currency
    AND raw.transaction_timestamp = enriched.transaction_timestamp

-- Get the data within 1 hour (from last audit to now)
-- The 1 hour comes from the execution of this script every hour, which will run on a DAG
WHERE
    enriched.ingest_timestamp BETWEEN watermark_cte.watermark_timestamp AND CURRENT_TIMESTAMP()
    AND raw.ingest_timestamp BETWEEN watermark_cte.watermark_timestamp AND CURRENT_TIMESTAMP();
