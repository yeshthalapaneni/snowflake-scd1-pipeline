-- Create Target Table
-- This table will store the latest customer records (SCD Type 1)

CREATE OR REPLACE TABLE customer (
    customer_id INTEGER,
    name STRING,
    email STRING,
    updated_at TIMESTAMP
);

-- Create Stored Procedure
-- Applies SCD Type 1 logic using MERGE

CREATE OR REPLACE PROCEDURE apply_scd1_customer()
RETURNS STRING
LANGUAGE SQL
AS
$$

MERGE INTO customer AS tgt
USING (
    SELECT
        customer_id,
        name,
        email,
        CURRENT_TIMESTAMP() AS updated_at
    FROM customer_stream
) AS src
ON tgt.customer_id = src.customer_id

WHEN MATCHED THEN UPDATE SET
    tgt.name = src.name,
    tgt.email = src.email,
    tgt.updated_at = src.updated_at

WHEN NOT MATCHED THEN INSERT (
    customer_id,
    name,
    email,
    updated_at
)
VALUES (
    src.customer_id,
    src.name,
    src.email,
    src.updated_at
);

RETURN 'SCD1 merge completed';

$$;

-- Notes:
-- This procedure overwrites existing records with new values (Type 1 behavior)
-- No history is maintained
-- The stream ensures only new data is processed
