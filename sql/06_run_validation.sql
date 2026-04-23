-- Validation Queries
-- Use these queries to verify pipeline behavior

-- Check landing table (data loaded by Snowpipe)
SELECT * FROM landing_customer ORDER BY ingestion_timestamp DESC;

-- Check stream (new changes waiting to be processed)
SELECT * FROM customer_stream;

-- Check target table (final SCD Type 1 result)
SELECT * FROM customer ORDER BY customer_id;

-- Check row counts
SELECT COUNT(*) AS landing_count FROM landing_customer;
SELECT COUNT(*) AS target_count FROM customer;

-- Verify updates (example for a specific customer)
SELECT * FROM customer WHERE customer_id = 1;

-- Notes:
-- After running the stored procedure or task, the stream should be consumed
-- and the target table should reflect the latest values only
