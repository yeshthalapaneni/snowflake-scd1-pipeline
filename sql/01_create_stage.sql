-- Create External Stage
-- This stage points Snowflake to the S3 location where files are stored

CREATE OR REPLACE STAGE customer_s3_stage
URL = 's3://your-bucket-name/customer_data/'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- Notes:
-- Replace 'your-bucket-name' with your actual S3 bucket
-- Ensure Snowflake has access to S3 (via storage integration or credentials)
