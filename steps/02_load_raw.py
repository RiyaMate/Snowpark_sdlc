#------------------------------------------------------------------------------
# Hands-On Lab: Data Engineering with Snowpark
# Script:       02_load_raw_co2.py
# Author:       Riya
# Last Updated: 2025-02-27
#
# Purpose:
#   - Loads CO₂ emissions data from an external S3 stage into Snowflake
#   - Ensures schemas and tables exist before loading
#   - Uses Snowpark for data ingestion and validation
#
# S3 Object Details:
#   - Name           : s3://aiemi/co2_daily_mlo.csv
#   - Size (bytes)   : 432016
#   - MD5            : ee457a3f42136bdc1ac7d888c17afd09
#   - Last Modified  : Thu, 27 Feb 2025 08:41:58 GMT
#------------------------------------------------------------------------------

import time
from snowflake.snowpark import Session
from snowflake.snowpark.types import StructType, StructField, IntegerType, FloatType

SNOWFLAKE_CONFIG = {
    "account": "qiqzsry-hv39958",  # Example: "xyz.snowflakecomputing.com"
    "user": "RMATE",
    "password": "Happyneu123@",
    "role": "CO2_ROLE",
    "warehouse": "CO2_WH",
    "database": "CO2_DB",
    "schema": "RAW_CO2"
}


# Define the expected CSV schema explicitly
csv_schema = StructType([
    StructField("Year", IntegerType()),
    StructField("Month", IntegerType()),
    StructField("Day", IntegerType()),
    StructField("Decimal_Date", FloatType()),
    StructField("CO2_ppm", FloatType())
])

# Define table structure and external stage reference
TABLE_DICT = {
    "co2": {
        "schema": "RAW_CO2",
        "tables": [
            {
                "name": "co2_daily_mlo",
                "location": "@CO2_DB.RAW_CO2.CO2_RAW_STAGE/co2_daily_mlo.csv"
            }
        ]
    }
}

def create_snowflake_session():
    """
    Establishes a Snowflake Snowpark session.
    """
    return Session.builder.configs(SNOWFLAKE_CONFIG).create()

def create_schema_if_not_exists(session, schema: str) -> None:
    """
    Ensures the schema exists before loading data.
    """
    create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    session.sql(create_schema_sql).collect()
    print(f"✅ Schema {schema} is ready.")

def create_table_if_not_exists(session, schema: str, tname: str) -> None:
    """
    Ensures the table exists before inserting data.
    """
    session.use_schema(schema)
    create_table_sql = f"""
    CREATE OR REPLACE TABLE {tname} (
        Year INTEGER,
        Month INTEGER,
        Day INTEGER,
        Decimal_Date FLOAT,
        CO2_ppm FLOAT
    )
    """
    session.sql(create_table_sql).collect()
    print(f"✅ Table {schema}.{tname} is ready.")

def configure_stage_file_format(session) -> None:
    """
    Configures the external stage with the correct CSV file format.
    """
    session.sql("""
        ALTER STAGE CO2_RAW_STAGE 
        SET FILE_FORMAT = (
            TYPE = CSV, 
            FIELD_OPTIONALLY_ENCLOSED_BY = '"', 
            NULL_IF = ('NULL', 'null', ''), 
            PARSE_HEADER = TRUE
        );
    """).collect()
    print("✅ External stage CO2_RAW_STAGE is correctly configured with PARSE_HEADER.")

def load_raw_table(session, tname: str, location: str, schema: str) -> None:
    """
    Loads a raw CSV table from an external stage into Snowflake.

    Parameters:
        session (Session): Active Snowpark session.
        tname (str): Target table name.
        location (str): External stage location of the CSV file.
        schema (str): Target schema in Snowflake.
    """
    # Ensure schema, table, and stage configuration exist before loading data.
    create_schema_if_not_exists(session, schema)
    create_table_if_not_exists(session, schema, tname)
    configure_stage_file_format(session)
    
    # Switch to the correct schema.
    session.use_schema(schema)

    # Read CSV data from the external stage
    df = session.read.schema(csv_schema).csv(location)
    
    # Copy data into Snowflake table with `ON_ERROR='CONTINUE'` to skip problematic rows
    df.copy_into_table(
        tname,
        on_error="CONTINUE"
    )
    
    # Add metadata to track file details.
    comment_text = (
        '{'
        '"origin": "sf_sit-is",'
        '"name": "snowpark_co2_quickstart",'
        '"version": {"major": 1, "minor": 0},'
        '"attributes": {'
        '    "is_quickstart": 1,'
        '    "source": "csv",'
        '    "file_name": "s3://aiemi/co2_daily_mlo.csv",'
        '    "file_size": 432016,'
        '    "file_md5": "ee457a3f42136bdc1ac7d888c17afd09",'
        '    "last_modified": "Thu, 27 Feb 2025 08:41:58 GMT"'
        '}'
        '}'
    )
    sql_command = f"COMMENT ON TABLE {tname} IS '{comment_text}';"
    session.sql(sql_command).collect()
    
    print(f"✅ Table {schema}.{tname} successfully loaded from {location}.")

def load_all_raw_tables(session) -> None:
    """
    Iterates over TABLE_DICT to load all raw CO₂ tables from the external stage.
    Scales the warehouse up for ingestion and then scales it back down.
    """
    # Scale up warehouse for better performance.
    session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()
    
    for key, config in TABLE_DICT.items():
        schema = config["schema"]
        for table_info in config["tables"]:
            tname = table_info["name"]
            location = table_info["location"]
            print(f"⏳ Loading table: {tname} from {location}")
            load_raw_table(session, tname=tname, location=location, schema=schema)
    
    # Scale down warehouse after ingestion to optimize cost.
    session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XSMALL").collect()

def validate_raw_tables(session) -> None:
    """
    Validates the loaded table by printing its column names.
    """
    for key, config in TABLE_DICT.items():
        schema = config["schema"]
        for table_info in config["tables"]:
            tname = table_info["name"]
            full_table_name = f"{schema}.{tname}"
            columns = session.table(full_table_name).columns
            print(f"✅ Table {full_table_name} columns:\n\t{columns}\n")

# ------------------------------------------------------------------------------
# Main Execution
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # Create a Snowpark session.
    session = create_snowflake_session()

    # Ensure the session is set to the correct database and schema.
    session.use_database("CO2_DB")
    session.use_schema("RAW_CO2")

    # Load CO2 data from external stage
    load_all_raw_tables(session)

    # Validate the loaded tables
    validate_raw_tables(session)

    # Close session
    session.close()
