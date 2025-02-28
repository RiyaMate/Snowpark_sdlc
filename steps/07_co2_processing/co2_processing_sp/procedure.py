import time
from snowflake.snowpark import Session
import snowflake.snowpark.types as T
import snowflake.snowpark.functions as F

def table_exists(session, schema='', name=''):
    """
    Check if a table exists in a given schema.
    """
    exists = session.sql(
        f"SELECT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{name}') AS TABLE_EXISTS"
    ).collect()[0]['TABLE_EXISTS']
    return exists

def create_daily_co2_metrics_table(session):
    """
    Create the DAILY_CO2_METRICS table if it doesn't exist.
    """
    METRICS_COLUMNS = [
        T.StructField("DATE", T.DateType()),
        T.StructField("COUNTRY", T.StringType()),
        T.StructField("MEASUREMENT_SITE", T.StringType()),
        T.StructField("AVG_CO2_PPM", T.DecimalType()),
        T.StructField("AVG_CO2_MGM3", T.DecimalType()),
        T.StructField("AVG_TEMPERATURE_CELSIUS", T.DecimalType()),
        T.StructField("AVG_PRESSURE_HPA", T.DecimalType()),
        T.StructField("CO2_PERCENT_CHANGE", T.DecimalType()),
    ]
    DAILY_CO2_METRICS_SCHEMA = T.StructType(METRICS_COLUMNS + [T.StructField("META_UPDATED_AT", T.TimestampType())])

    session.create_dataframe([[None] * len(DAILY_CO2_METRICS_SCHEMA.names)], schema=DAILY_CO2_METRICS_SCHEMA) \
           .na.drop() \
           .write.mode('overwrite').save_as_table('ANALYTICS_CO2.DAILY_CO2_METRICS')

def merge_daily_co2_metrics(session):
    """
    Merge new CO₂ data into the DAILY_CO2_METRICS table.
    """
    _ = session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XLARGE WAIT_FOR_COMPLETION = TRUE").collect()

    print(f"{session.table('HARMONIZED_CO2.CO2_HARMONIZED_V_STREAM').count()} records in stream")

    co2_stream_dates = session.table("HARMONIZED_CO2.CO2_HARMONIZED_V_STREAM") \
        .select(F.col("DECIMAL_DATE").alias("DATE")).distinct()
    
    co2_metrics = session.table("HARMONIZED_CO2.CO2_HARMONIZED_V_STREAM") \
        .group_by(F.col('DECIMAL_DATE'), F.col('COUNTRY'), F.col('MEASUREMENT_SITE')) \
        .agg(
            F.avg(F.col("CO2_PPM")).alias("AVG_CO2_PPM"),
            F.avg(F.call_udf("ANALYTICS_CO2.PPM_TO_MG_M3_UDF", F.col("CO2_PPM"))).alias("AVG_CO2_MGM3"),
            F.avg(F.col("AVG_TEMPERATURE_CELSIUS")).alias("AVG_TEMPERATURE_CELSIUS"),
            F.avg(F.col("AVG_PRESSURE_HPA")).alias("AVG_PRESSURE_HPA"),
            F.avg(F.call_udf("ANALYTICS_CO2.CO2_PERCENT_CHANGE_UDF", F.lag("CO2_PPM").over(), F.col("CO2_PPM"))).alias("CO2_PERCENT_CHANGE")
        ) \
        .select(
            F.col("DECIMAL_DATE").alias("DATE"),
            F.col("COUNTRY"),
            F.col("MEASUREMENT_SITE"),
            F.col("AVG_CO2_PPM"),
            F.col("AVG_CO2_MGM3"),
            F.col("AVG_TEMPERATURE_CELSIUS"),
            F.col("AVG_PRESSURE_HPA"),
            F.col("CO2_PERCENT_CHANGE")
        )

    # Prepare merge conditions
    cols_to_update = {c: co2_metrics[c] for c in co2_metrics.schema.names}
    metadata_col_to_update = {"META_UPDATED_AT": F.current_timestamp()}
    updates = {**cols_to_update, **metadata_col_to_update}

    # Merge into DAILY_CO2_METRICS
    dcm = session.table("ANALYTICS_CO2.DAILY_CO2_METRICS")
    dcm.merge(co2_metrics,
              (dcm['DATE'] == co2_metrics['DATE']) &
              (dcm['COUNTRY'] == co2_metrics['COUNTRY']) &
              (dcm['MEASUREMENT_SITE'] == co2_metrics['MEASUREMENT_SITE']),
              [F.when_matched().update(updates),
               F.when_not_matched().insert(updates)])

    _ = session.sql("ALTER WAREHOUSE CO2_WH SET WAREHOUSE_SIZE = XSMALL").collect()

def main(session: Session) -> str:
    """
    Main function to process CO₂ data.
    """
    if not table_exists(session, schema="ANALYTICS_CO2", name="DAILY_CO2_METRICS"):
        create_daily_co2_metrics_table(session)
    
    merge_daily_co2_metrics(session)
    
    return "Successfully processed DAILY_CO2_METRICS"

if __name__ == '__main__':
    with Session.builder.getOrCreate() as session:
        import sys
        if len(sys.argv) > 1:
            print(main(session, *sys.argv[1:]))  # type: ignore
        else:
            print(main(session))  # type: ignore
