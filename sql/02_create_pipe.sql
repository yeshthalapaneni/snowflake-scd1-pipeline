-- Create Landing Table
-- This table receives data loaded by Snowpipe

CREATE OR REPLACE TABLE landing_customer (
    customer_id INTEGER,
    name STRING,
    email STRING,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create Snowpipe
-- Automatically loads new files from S3 into the landing table

CREATE OR REPLACE PIPE customer_snowpipe
AUTO_INGEST = TRUE
AS
COPY INTO landing_customer (customer_id, name, email)
FROM (
    SELECT
        $1::INTEGER,
        $2::STRING,
        $3::STRING
    FROM @customer_s3_stage
)
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
ON_ERROR = 'CONTINUE';

-- Notes:
-- AUTO_INGEST requires S3 event notifications to be configured
-- Alternatively, you can manually trigger ingestion using:
-- ALTER PIPE customer_snowpipe REFRESH;
