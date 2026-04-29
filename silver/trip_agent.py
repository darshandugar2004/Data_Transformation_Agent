from pyspark import pipelines as dp
from pyspark.sql import functions as F
from trip_utils import agent_transform

# ======================================================================
@dp.view(
    name="trips_silver_staging", comment="Transformed trips data ready for CDC upsert"
)
def trips_silver():
    df_bronze = spark.readStream.table("transportation.bronze.trips")
    df_silver = df_bronze.withColumn("passenger_type", F.lower("passenger_type"))

    # Define target schema (you can also fetch from gold table)
    target_schema = {
        "id": "string",
        "business_date": "date",
        "city_id": "string",
        "passenger_category": "string",
        "distance_kms": "int",
        "sales_amt": "int",
        "passenger_rating": "int",
        "driver_rating": "int",
	    "silver_ingest_timestamp": "timestamp",
        "bronze_ingest_timestamp": "timestamp"
    }

    # 🧠 Apply agent
    df_silver = agent_transform(df_bronze, target_schema)

    print("silver schema: 1 ", df_silver.schema)
    return df_silver


dp.create_streaming_table(
    name="transportation.silver.trips",
    comment="Cleaned and validated orders with CDC upsert capability",
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
    },
)

dp.create_auto_cdc_flow(
    target="transportation.silver.trips",
    source="trips_silver_staging",
    keys=["id"],
    sequence_by=F.col("silver_ingest_timestamp"),
    stored_as_scd_type=1,
    except_column_list=[],
)
