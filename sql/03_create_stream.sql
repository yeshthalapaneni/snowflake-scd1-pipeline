-- Create Stream on Landing Table
-- This stream tracks new and changed records from the landing table

CREATE OR REPLACE STREAM customer_stream
ON TABLE landing_customer
APPEND_ONLY = FALSE;

-- Notes:
-- APPEND_ONLY = FALSE allows tracking of inserts and updates
-- The stream captures only new data since the last consumption
-- It enables incremental processing instead of full table scans
