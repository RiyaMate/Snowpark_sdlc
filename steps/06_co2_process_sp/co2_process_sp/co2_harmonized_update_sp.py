#------------------------------------------------------------------------------
# Snowflake CO₂ Data Processing Pipeline
# Script:       co2_harmonized_update_sp.py
# Author:       Riya
# Last Updated: 2025-02-27
#------------------------------------------------------------------------------
#
# Purpose:
#   - Merges CO₂ changes from HARMONIZED_CO2.CO2_HARMONIZED_V_STREAM
#   - Updates the HARMONIZED_CO2.CO2_PROCESSED table
#   - Ensures incremental processing using Snowflake Streams
#
# SNOWFLAKE ADVANTAGE:
#   - Python Stored Procedures in Snowpark
#   - Streams for Change Data Capture (CDC)
#   - MERGE for efficient upserts
#------------------------------------------------------------------------------

import time
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    """
    Checks if a table exists in a given schema.
    """
    exists = session.sql(f"SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}') AS TABLE_EXISTS").collect()[0]['TABLE_EXISTS']
    return exists

def create_co2_table(session):
    """
    Creates the CO2_PROCESSED table based on CO2_HARMONIZED_V schema.
    """
    _ = session.sql("CREATE TABLE HARMONIZED_CO2.CO2_PROCESSED LIKE HARMONIZED_CO2.CO2_HARMONIZED_V").collect()
    _ = session.sql("ALTER TABLE HARMONIZED_CO2.CO2_PROCESSED ADD COLUMN META_UPDATED_AT TIMESTAMP").collect()

def create_co2_stream(session):
    """
    Creates a stream to track changes in CO2_PROCESSED table.
    """
    _ = session.sql("CREATE STREAM HARMONIZED_CO2.CO2_PROCESSED_STREAM ON TABLE HARMONIZED_CO2.CO2_PROCESSED").collect()

def merge_co2_updates(session):
    """
    Merges new CO₂ data from CO2_HARMONIZED_V_STREAM into CO2_PROCESSED.
    """
    # Scale up for ingestion
    _ = session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    source = session.table("HARMONIZED_CO2.CO2_HARMONIZED_V_STREAM")
    target = session.table("HARMONIZED_CO2.CO2_PROCESSED")

    cols_to_update = {c: source[c] for c in source.schema.names if "META" not in c}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    # Perform merge
    target.merge(
        source,
        target["YEAR"] == source["YEAR"],  # Assuming YEAR as a unique key
        [F.when_matched().update(updates), F.when_not_matched().insert(updates)]
    )

    # Scale down after processing
    _ = session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XSMALL").collect()

def main(session: Session) -> str:
    """
    Main function for processing CO₂ data.
    - Ensures tables and streams exist
    - Merges new data into CO2_PROCESSED
    """
    # Create the CO2_PROCESSED table and stream if they don't exist
    if not table_exists(session, schema="HARMONIZED_CO2", name="CO2_PROCESSED"):
        create_co2_table(session)
        create_co2_stream(session)

    # Process new CO₂ data incrementally
    merge_co2_updates(session)

    return "✅ Successfully processed CO₂ data"

# ------------------------------------------------------------------------------  
# Local Debugging
# ------------------------------------------------------------------------------  
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.getOrCreate() as session:
        import sys
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # type: ignore
        else:
            print(main(session))  # type: ignore
