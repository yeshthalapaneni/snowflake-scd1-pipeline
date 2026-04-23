-- Create Task to automate SCD Type 1 processing

-- Make sure a warehouse is available
CREATE WAREHOUSE IF NOT EXISTS CUSTOMER_WH
WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;

-- Create Task
-- This task runs the stored procedure on a schedule

CREATE OR REPLACE TASK customer_scd1_task
WAREHOUSE = CUSTOMER_WH
SCHEDULE = '1 MINUTE'
AS
CALL apply_scd1_customer();

-- Start the task
ALTER TASK customer_scd1_task RESUME;

-- Notes:
-- You can change the schedule as needed (e.g., every 5 minutes, hourly)
-- To stop the task:
-- ALTER TASK customer_scd1_task SUSPEND;
-- To check task status:
-- SHOW TASKS;
