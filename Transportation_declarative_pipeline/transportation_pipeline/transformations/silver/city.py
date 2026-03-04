from pyspark.sql import functions as F
from pyspark import pipelines as dp

@dp.materialized_view(
    name="transportation.silver.city",
    comment="silver table for city data",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_silver():
    df_bronze = spark.read.table("transportation.bronze.city")
    df_silver = df_bronze.select(
        F.col("city_id").alias("city_id"),
        F.col("city_name").alias("city_name"),
        F.col("ingest_datetime").alias("bronze_ingest_timestamp")
    )

    df_silver = df_silver.withColumn("silver_ingest_timestamp", F.current_timestamp())
    return df_silver
        
                                 
                                 
    