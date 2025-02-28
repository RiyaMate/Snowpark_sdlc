/*-----------------------------------------------------------------------------
Hands-On Lab: Data Engineering with Snowpark
Script:       01_setup_snowflake.sql
Author:       Riya
Last Updated: 27-Feb-2025
-----------------------------------------------------------------------------*/

-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions (If Required)
-- ----------------------------------------------------------------------------
-- See Getting Started section in Third-Party Packages:
-- https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started

-- ----------------------------------------------------------------------------
-- Step #2: Create Account-Level Objects (Roles & Privileges)
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Set current user variable
SET MY_USER = CURRENT_USER();

-- Create a dedicated role for CO2 data pipeline operations
CREATE OR REPLACE ROLE CO2_ROLE;
GRANT ROLE CO2_ROLE TO ROLE SYSADMIN;
GRANT ROLE CO2_ROLE TO USER IDENTIFIER($MY_USER);

-- Grant necessary account-level privileges
GRANT EXECUTE TASK ON ACCOUNT TO ROLE CO2_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE CO2_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE CO2_ROLE;

-- ----------------------------------------------------------------------------
-- Step #3: Create Database & Warehouse for CO2 Data Processing
-- ----------------------------------------------------------------------------
CREATE OR REPLACE DATABASE CO2_DB;
GRANT OWNERSHIP ON DATABASE CO2_DB TO ROLE CO2_ROLE;

CREATE OR REPLACE WAREHOUSE CO2_WH 
    WAREHOUSE_SIZE = XSMALL 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE;

GRANT OWNERSHIP ON WAREHOUSE CO2_WH TO ROLE CO2_ROLE;

-- ----------------------------------------------------------------------------
-- Step #4: Create Schemas for CO2 Data Processing
-- ----------------------------------------------------------------------------
USE ROLE CO2_ROLE;
USE WAREHOUSE CO2_WH;
USE DATABASE CO2_DB;

-- Create schemas for different stages of processing
CREATE OR REPLACE SCHEMA RAW_CO2;        -- Stores raw CO2 emissions data
CREATE OR REPLACE SCHEMA HARMONIZED_CO2; -- Stores transformed & cleaned data
CREATE OR REPLACE SCHEMA ANALYTICS_CO2;  -- Stores analytical tables

-- Assign ownership of schemas to CO2_ROLE
GRANT OWNERSHIP ON SCHEMA RAW_CO2 TO ROLE CO2_ROLE;
GRANT OWNERSHIP ON SCHEMA HARMONIZED_CO2 TO ROLE CO2_ROLE;
GRANT OWNERSHIP ON SCHEMA ANALYTICS_CO2 TO ROLE CO2_ROLE;

-- ----------------------------------------------------------------------------
-- Step #5: Create External Stage for CO2 Data Ingestion
-- ----------------------------------------------------------------------------
USE SCHEMA RAW_CO2;

-- Define file format for CSV data
CREATE OR REPLACE FILE FORMAT CO2_CSV_FORMAT 
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '');

-- Create external stage pointing to the S3 bucket (Ensure correct permissions in AWS)
CREATE OR REPLACE STAGE CO2_RAW_STAGE 
    URL = 's3://aiemi/' -- Corrected stage path (folder level)
    FILE_FORMAT = CO2_CSV_FORMAT;

-- Verify that Snowflake can list files in the external stage
LIST @CO2_RAW_STAGE;

-- ----------------------------------------------------------------------------
-- Step #6: Create User-Defined Functions (UDFs) for CO2 Data Processing
-- ----------------------------------------------------------------------------
USE SCHEMA ANALYTICS_CO2;

-- UDF to convert CO2 concentration from parts per million (ppm) to mg/m³
CREATE OR REPLACE FUNCTION PPM_TO_MG_M3_UDF(PPM FLOAT)
RETURNS FLOAT
LANGUAGE SQL
    AS
$$
    PPM * 1.96  -- Approximate conversion factor for CO2 at standard conditions
$$;

-- UDF to calculate daily CO2 percentage change
CREATE OR REPLACE FUNCTION CO2_PERCENT_CHANGE_UDF(PREVIOUS_VALUE FLOAT, CURRENT_VALUE FLOAT)
RETURNS FLOAT
LANGUAGE SQL
    AS
$$
    CASE 
        WHEN PREVIOUS_VALUE IS NULL OR PREVIOUS_VALUE = 0 THEN NULL
        ELSE ((CURRENT_VALUE - PREVIOUS_VALUE) / PREVIOUS_VALUE) * 100
    END
$$;

-- ----------------------------------------------------------------------------
-- Step #7: Grant Necessary Privileges to CO2_ROLE
-- ----------------------------------------------------------------------------
GRANT USAGE ON DATABASE CO2_DB TO ROLE CO2_ROLE;

-- Grant usage and object creation privileges on RAW_CO2
GRANT USAGE, CREATE TABLE, CREATE STAGE, CREATE FUNCTION ON SCHEMA RAW_CO2 TO ROLE CO2_ROLE;


-- Grant privileges on HARMONIZED_CO2
GRANT USAGE, CREATE TABLE ON SCHEMA HARMONIZED_CO2 TO ROLE CO2_ROLE;


-- Grant privileges on ANALYTICS_CO2
GRANT USAGE, CREATE FUNCTION ON SCHEMA ANALYTICS_CO2 TO ROLE CO2_ROLE;

SHOW FILE FORMATS;
SHOW SCHEMAS IN DATABASE CO2_DB;
CREATE SCHEMA CO2_DB.HARMONIZED_CO2;



