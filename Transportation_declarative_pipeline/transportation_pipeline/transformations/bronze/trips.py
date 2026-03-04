from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE_PATH = "s3://spark-declarative-cabs-avi360/data-store/trips/"

@dp.table(
    name = "transportation.bronze.trip",
    comment = "Loading raw CSV data to bronze schema",
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "source_format": "csv",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def orders_bronze():
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", True)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.maxFilesPerTrigger", 100)
        .load(SOURCE_PATH)
    )

    df = df.withColumnRenamed("distance_travelled(km)", "distance_travelled_km")
    df = df.withColumn("file_path", F.col("_metadata.file_path")).withColumn("ingest_timestamp", F.current_timestamp())

    return df