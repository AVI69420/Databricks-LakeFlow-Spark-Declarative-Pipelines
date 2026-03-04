from pyspark.sql import functions as F
from pyspark import pipelines as dp

SOURCE_PATH = "s3://spark-declarative-cabs-avi360/data-store/city" 

@dp.materialized_view(
    name="transportation.bronze.city",
    comment="Bronze table for city data",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def city_bronze():
    df = spark.read.format("csv").option("header", "true").option("inferSchema","true").option("mode", "PERMISSIVE").option("mergeSchema", "true").option("columnNameOfCorruptRecord", "_corrupt_record").load(SOURCE_PATH)

    df = df.withColumn("filename", F.col("_metadata.file_name")).withColumn("ingest_datetime", F.current_timestamp())
    
    return df