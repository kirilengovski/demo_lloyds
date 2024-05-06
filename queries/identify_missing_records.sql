-- Select all columns from the raw_data_audit table
SELECT
    raw.*
FROM
    raw_data_audit AS raw
LEFT JOIN
    enriched_data_audit AS enriched
ON
    raw.transaction_id = enriched.transaction_id
    AND raw.amount = enriched.amount
    AND raw.currency = enriched.currency
    AND raw.transaction_timestamp = enriched.transaction_timestamp

-- Filter records where there is no corresponding match in the enriched table
WHERE
    enriched.transaction_id IS NULL;