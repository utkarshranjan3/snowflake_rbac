USE ROLE ACCOUNTADMIN;

-- 1. Create Warehouse
CREATE WAREHOUSE IF NOT EXISTS uber_wh 
    WAREHOUSE_SIZE = 'X-SMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE;

-- 2. Create Roles
CREATE ROLE IF NOT EXISTS uber_admin_role;
CREATE ROLE IF NOT EXISTS uber_analyst_role;

-- 3. Grant Warehouse Privileges
GRANT USAGE ON WAREHOUSE uber_wh TO ROLE uber_admin_role;
GRANT USAGE ON WAREHOUSE uber_wh TO ROLE uber_analyst_role;

-- 4. Create Database and Schemas
CREATE DATABASE IF NOT EXISTS uber_db;
CREATE SCHEMA IF NOT EXISTS uber_db.raw;
CREATE SCHEMA IF NOT EXISTS uber_db.silver;
CREATE SCHEMA IF NOT EXISTS uber_db.gold;

-- 5. Grant Schema Privileges (RBAC Principle of Least Privilege)
GRANT USAGE ON DATABASE uber_db TO ROLE uber_admin_role; 
GRANT USAGE ON DATABASE uber_db TO ROLE uber_analyst_role;
GRANT ALL PRIVILEGES ON SCHEMA uber_db.raw TO ROLE uber_admin_role;
GRANT ALL PRIVILEGES ON SCHEMA uber_db.silver TO ROLE uber_admin_role;
GRANT ALL PRIVILEGES ON SCHEMA uber_db.gold TO ROLE uber_admin_role;

GRANT USAGE ON SCHEMA uber_db.silver TO ROLE uber_analyst_role;
GRANT USAGE ON SCHEMA uber_db.gold TO ROLE uber_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA uber_db.silver TO ROLE uber_analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA uber_db.gold TO ROLE uber_analyst_role


-- 1. Switch back to a privileged role
USE ROLE ACCOUNTADMIN;

-- 2. Find your exact Snowflake username (if you aren't sure)
SELECT CURRENT_USER();

-- 3. Grant the role to your user 
-- (Replace 'YOUR_USERNAME' with the username returned by the query above)
GRANT ROLE uber_admin_role TO USER UTK;

-- 4. Optional: Also grant it to SYSADMIN so system administrators can manage it
GRANT ROLE uber_admin_role TO ROLE SYSADMIN;

-- 5. Now you can successfully switch to your role
USE ROLE uber_admin_role;




USE ROLE uber_admin_role;
USE WAREHOUSE uber_wh;
USE DATABASE uber_db;

-- 1. Create File Format & Internal Stage
CREATE OR REPLACE FILE FORMAT raw.json_format 
    TYPE = 'JSON' 
    STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE STAGE raw.uber_stage 
    FILE_FORMAT = raw.json_format;

-- 2. Raw Table with Advanced Table Properties (Time Travel / Archiving)
-- Interview Concept: DATA_RETENTION_TIME_IN_DAYS allows querying history up to 90 days (Enterprise edition).
CREATE OR REPLACE TABLE raw.raw_driver_updates (
    driver_id STRING,
    driver_name STRING,
    city STRING,
    updated_at TIMESTAMP_NTZ
)
DATA_RETENTION_TIME_IN_DAYS = 90;

-- 3. Silver SCD Type 2 Dimension Table with Clustering
-- Interview Concept: Clustering Keys optimize queries filtered by high-cardinality columns like driver_id.
CREATE OR REPLACE TABLE silver.dim_driver_scd2 (
    driver_key NUMBER IDENTITY(1,1),
    driver_id STRING,
    driver_name STRING,
    city STRING,
    effective_start_date TIMESTAMP_NTZ,
    effective_end_date TIMESTAMP_NTZ,
    is_current BOOLEAN
)
CLUSTER BY (driver_id)
DATA_RETENTION_TIME_IN_DAYS = 30;UBER_DB.RAW.UBER_STAGEUBER_DB.RAW.UBER_STAGEUBER_DB.RAW.UBER_STAGE



-- 1. Recreate the Pipe with MATCH_BY_COLUMN_NAME
CREATE OR REPLACE PIPE raw.uber_driver_pipe
AUTO_INGEST = TRUE
AS
COPY INTO raw.raw_driver_updates
FROM @raw.uber_stage
FILE_FORMAT = (FORMAT_NAME = 'raw.json_format')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


-- Create a stream on the raw ingestion table to track incoming changes
CREATE OR REPLACE STREAM raw.driver_stream 
ON TABLE raw.raw_driver_updates;


CREATE OR REPLACE PROCEDURE silver.sp_process_driver_scd2()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    -- Step A: Close out existing active records if a new update arrives for the same driver
    UPDATE silver.dim_driver_scd2 AS target
    SET target.effective_end_date = source.updated_at,
        target.is_current = FALSE
    FROM raw.driver_stream AS source
    WHERE target.driver_id = source.driver_id
      AND target.is_current = TRUE
      AND source.METADATA$ACTION = 'INSERT';

    -- Step B: Insert the new updated/brand-new record with is_current = TRUE
    INSERT INTO silver.dim_driver_scd2 (
        driver_id, driver_name, city, effective_start_date, effective_end_date, is_current
    )
    SELECT 
        source.driver_id,
        source.driver_name,
        source.city,
        source.updated_at,
        NULL::TIMESTAMP_NTZ,
        TRUE
    FROM raw.driver_stream AS source
    WHERE source.METADATA$ACTION = 'INSERT';

    RETURN 'SCD Type 2 processed successfully';
END;
$$;


-- Create a scheduled task to run the CDC process every hour
CREATE OR REPLACE TASK silver.run_driver_scd2_task
    WAREHOUSE = uber_wh
    SCHEDULE = '1 MINUTE'
WHEN
    SYSTEM$STREAM_HAS_DATA('raw.driver_stream')
AS
    CALL silver.sp_process_driver_scd2();

-- Tasks are created suspended by default. Resume the task:
ALTER TASK silver.run_driver_scd2_task RESUME;

-- 1. Switch to account admin role
USE ROLE ACCOUNTADMIN;

-- 2. Grant the global EXECUTE TASK privilege to your admin role
GRANT EXECUTE TASK ON ACCOUNT TO ROLE uber_admin_role;


USE ROLE uber_admin_role;
USE WAREHOUSE uber_wh;
USE DATABASE uber_db;

-- Now manual execution will work
EXECUTE TASK silver.run_driver_scd2_task;



-- 1. Query table state as of 30 minutes ago (Time Travel)
SELECT * 
FROM silver.dim_driver_scd2 
AT(OFFSET => -60*30);

-- 2. Query table state before a specific SQL statement using query ID
-- SELECT * FROM silver.dim_driver_scd2 BEFORE(STATEMENT => '01a2b3c4-0001-...');

-- 3. Clone a table instantly (Zero-Copy Clone - zero storage overhead initially)
CREATE TABLE silver.dim_driver_scd2_clone 
CLONE silver.dim_driver_scd2;


-- 1. Switch back to a privileged role
USE ROLE ACCOUNTADMIN;

-- 2. Grant the analyst role to your user 
-- (Replace 'YOUR_USERNAME' with your actual Snowflake username)
GRANT ROLE uber_analyst_role TO USER UTK;

-- 3. Now you can successfully switch to your analyst role
USE ROLE uber_analyst_role;






USE ROLE uber_analyst_role;
USE WAREHOUSE uber_wh;
USE DATABASE uber_db;

ALTER PIPE raw.uber_driver_pipe REFRESH;

-- Query current active drivers by city for a Snowsight Dashboard chart
SELECT 
    city,
    COUNT(driver_id) AS total_active_drivers
FROM silver.dim_driver_scd2
WHERE is_current = TRUE
GROUP BY city
ORDER BY total_active_drivers DESC;


select * from raw.driver_stream
































