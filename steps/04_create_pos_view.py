#------------------------------------------------------------------------------
# Snowflake CO₂ Data Processing Pipeline
# Script:       04_create_co2_view.py
# Author:       Riya
# Last Updated: 2025-02-27
#------------------------------------------------------------------------------
#
# Purpose:
#   - Creates a harmonized view of CO₂ emissions data.
#   - Ensures schemas, tables, and views exist.
#   - Uses Snowpark DataFrame API for transformation.
#   - Sets up a stream for incremental processing.
#
# SNOWFLAKE ADVANTAGE:
#   - Snowpark DataFrame API for efficient data transformation.
#   - Streams for Change Data Capture (CDC).
#   - Views for logical data modeling.
#------------------------------------------------------------------------------

from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

# Snowflake Connection Configuration
SNOWFLAKE_CONFIG = {
    "account": "qiqzsry-hv39958",  # Example: "xyz.snowflakecomputing.com"
    "user": "RMATE",
    "password": "Happyneu123@",
    "role": "CO2_ROLE",
    "warehouse": "CO2_WH",
    "database": "CO2_DB",
    "schema": "RAW_CO2"
}

def create_snowflake_session():
    """
    Establishes a Snowflake Snowpark session.
    """
    session = Session.builder.configs(SNOWFLAKE_CONFIG).create()
    session.sql("USE WAREHOUSE CO2_WH").collect()  # Activate the warehouse
    print("✅ Snowflake session created & warehouse activated.")
    return session

def ensure_schema_exists(session, schema_name):
    """
    Ensures that the target schema exists before using it.
    """
    try:
        session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()
        print(f"✅ Schema '{schema_name}' is ready.")
    except Exception as e:
        print(f"❌ Error ensuring schema '{schema_name}': {e}")

def ensure_table_exists(session, schema, table_name):
    """
    Ensures that the required table exists before querying it.
    """
    tables = [row[1] for row in session.sql(f"SHOW TABLES IN {schema}").collect()]
    if table_name not in tables:
        print(f"⚠️ Table '{schema}.{table_name}' does not exist. Creating it now...")
        session.sql(f"""
            CREATE OR REPLACE TABLE {schema}.{table_name} (
                YEAR INTEGER,
                MONTH INTEGER,
                DAY INTEGER,
                DECIMAL_DATE FLOAT,
                CO2_PPM FLOAT,
                CO2_DAILY_CHANGE FLOAT
            )
        """).collect()
        print(f"✅ Table '{schema}.{table_name}' successfully created.")

def ensure_permissions(session):
    """
    Grants necessary permissions to CO2_ROLE.
    """
    try:
        session.sql("GRANT USAGE ON DATABASE CO2_DB TO ROLE CO2_ROLE").collect()
        session.sql("GRANT USAGE ON SCHEMA RAW_CO2 TO ROLE CO2_ROLE").collect()
        session.sql("GRANT USAGE ON SCHEMA HARMONIZED_CO2 TO ROLE CO2_ROLE").collect()
        session.sql("GRANT CREATE VIEW ON SCHEMA HARMONIZED_CO2 TO ROLE CO2_ROLE").collect()
        session.sql("GRANT SELECT ON FUTURE VIEWS IN SCHEMA HARMONIZED_CO2 TO ROLE CO2_ROLE").collect()
        session.sql("GRANT SELECT ON TABLE RAW_CO2.CO2_DAILY_MLO TO ROLE CO2_ROLE").collect()
        print("✅ Permissions granted to CO2_ROLE.")
    except Exception as e:
        print(f"❌ Error granting permissions: {e}")

def create_co2_view(session):
    """
    Creates a harmonized view of CO₂ data.
    """
    ensure_schema_exists(session, 'HARMONIZED_CO2')
    ensure_table_exists(session, 'RAW_CO2', 'CO2_DAILY_MLO')

    session.use_schema('HARMONIZED_CO2')

    # Load raw CO₂ data
    co2_data = session.table("RAW_CO2.CO2_DAILY_MLO")

    # 🔍 DEBUG: Print available columns
    print("✅ Available columns in CO2_DAILY_MLO:", co2_data.columns)

    # Select only available columns
    columns_to_select = ["YEAR", "MONTH", "DAY", "DECIMAL_DATE", "CO2_PPM"]
    
    if "CO2_DAILY_CHANGE" in co2_data.columns:
        columns_to_select.append("CO2_DAILY_CHANGE")
    else:
        print("⚠️ Warning: Column 'CO2_DAILY_CHANGE' is missing. Proceeding without it.")

    co2_data = co2_data.select(*[F.col(c) for c in columns_to_select])

    # Create or replace the CO₂ harmonized view
    co2_data.create_or_replace_view("CO2_HARMONIZED_V")
    print("✅ View 'CO2_HARMONIZED_V' successfully created.")


def create_co2_view_stream(session):
    """
    Creates a stream to track incremental changes in the CO2_HARMONIZED_V view.
    """
    ensure_schema_exists(session, 'HARMONIZED_CO2')
    session.use_schema('HARMONIZED_CO2')

    try:
        session.sql("""
            CREATE OR REPLACE STREAM CO2_HARMONIZED_V_STREAM 
            ON VIEW CO2_HARMONIZED_V 
            SHOW_INITIAL_ROWS = TRUE
        """).collect()
        print("✅ Stream 'CO2_HARMONIZED_V_STREAM' successfully created.")
    except Exception as e:
        print(f"❌ Error creating stream: {e}")

def test_co2_view(session):
    """
    Tests the CO₂ harmonized view by displaying the first 5 rows.
    """
    ensure_schema_exists(session, 'HARMONIZED_CO2')
    session.use_schema('HARMONIZED_CO2')

    try:
        co2_view = session.table("CO2_HARMONIZED_V")
        co2_view.limit(5).show()
    except Exception as e:
        print(f"❌ Error querying CO2_HARMONIZED_V: {e}")

# ---------------------------------------------------------------------------
# Main Execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Create a Snowpark session
    session = create_snowflake_session()

    # Ensure required schemas, tables, and permissions exist
    ensure_permissions(session)

    # Create harmonized CO₂ view
    create_co2_view(session)

    # Create stream for incremental changes
    create_co2_view_stream(session)

    # Test the view (optional)
    # test_co2_view(session)

    # Close session
    session.close()
