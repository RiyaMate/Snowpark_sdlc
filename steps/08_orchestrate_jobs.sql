------------------------------------------------------------------------------
-- 1) Use the correct role, warehouse, database, and schema
------------------------------------------------------------------------------
USE ROLE CO2_ROLE;               -- Must have OWN or OPERATE privilege on tasks
USE WAREHOUSE CO2_WH;            -- Warehouse for tasks
USE DATABASE CO2_DB;
USE SCHEMA HARMONIZED_CO2;       -- Place BOTH tasks in the same schema

------------------------------------------------------------------------------
-- 2) (Optional) Show existing tasks before modifying
------------------------------------------------------------------------------
SHOW TASKS IN SCHEMA CO2_DB.HARMONIZED_CO2;

------------------------------------------------------------------------------
-- 3) Suspend the child task first (if it exists)
------------------------------------------------------------------------------
ALTER TASK IF EXISTS CO2_DAILY_METRICS_UPDATE_TASK SUSPEND;
CALL SYSTEM$WAIT(15);

------------------------------------------------------------------------------
-- 4) Suspend the root task
------------------------------------------------------------------------------
ALTER TASK IF EXISTS CO2_HARMONIZED_UPDATE_TASK SUSPEND;
CALL SYSTEM$WAIT(20);

------------------------------------------------------------------------------
-- 5) Confirm tasks are suspended
------------------------------------------------------------------------------
SHOW TASKS IN SCHEMA CO2_DB.HARMONIZED_CO2;

------------------------------------------------------------------------------
-- 6) Drop both tasks in reverse order: child first, then root
------------------------------------------------------------------------------
DROP TASK IF EXISTS CO2_DAILY_METRICS_UPDATE_TASK;
DROP TASK IF EXISTS CO2_HARMONIZED_UPDATE_TASK;

------------------------------------------------------------------------------
-- 7) Create the root task (triggered by a stream in HARMONIZED_CO2)
------------------------------------------------------------------------------
CREATE OR REPLACE TASK CO2_HARMONIZED_UPDATE_TASK
  WAREHOUSE = CO2_WH
  WHEN SYSTEM$STREAM_HAS_DATA('CO2_DB.HARMONIZED_CO2.CO2_HARMONIZED_V_STREAM')
AS
CALL CO2_DB.HARMONIZED_CO2.CO2_HARMONIZED_UPDATE_SP();

------------------------------------------------------------------------------
-- 8) Create the dependent (child) task in the SAME schema,
--    referencing the root task with AFTER.
------------------------------------------------------------------------------
CREATE OR REPLACE TASK CO2_DAILY_METRICS_UPDATE_TASK
  WAREHOUSE = CO2_WH
  AFTER CO2_HARMONIZED_UPDATE_TASK
  WHEN SYSTEM$STREAM_HAS_DATA('CO2_DB.HARMONIZED_CO2.CO2_PROCESSED_STREAM')
AS
CALL CO2_DB.HARMONIZED_CO2.CO2_DAILY_METRICS_UPDATE_SP();

------------------------------------------------------------------------------
-- 9) Resume the root task, then the child task
------------------------------------------------------------------------------
ALTER TASK CO2_HARMONIZED_UPDATE_TASK RESUME;
CALL SYSTEM$WAIT(10);

ALTER TASK CO2_DAILY_METRICS_UPDATE_TASK RESUME;

------------------------------------------------------------------------------
-- 10) (Optional) Manually execute the root task for immediate testing
------------------------------------------------------------------------------
EXECUTE TASK CO2_HARMONIZED_UPDATE_TASK;

------------------------------------------------------------------------------
-- 11) Show tasks again to confirm
------------------------------------------------------------------------------
SHOW TASKS IN SCHEMA CO2_DB.HARMONIZED_CO2;

------------------------------------------------------------------------------
-- 12) Check recent task history (last day)
------------------------------------------------------------------------------
SELECT *
FROM TABLE(
    INFORMATION_SCHEMA.TASK_HISTORY(
        SCHEDULED_TIME_RANGE_START => DATEADD('DAY', -1, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 100
    )
)
ORDER BY SCHEDULED_TIME DESC;
