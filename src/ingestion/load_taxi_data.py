from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp


def create_spark_session(app_name: str = "NYC Taxi Data Ingestion") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_taxi_parquet(spark: SparkSession, input_path: str):
    df = spark.read.parquet(input_path)

    # Normalize timestamp fields for Yellow Taxi schema
    if "tpep_pickup_datetime" in df.columns:
        df = df.withColumn(
            "pickup_datetime",
            to_timestamp(col("tpep_pickup_datetime"))
        )

    if "tpep_dropoff_datetime" in df.columns:
        df = df.withColumn(
            "dropoff_datetime",
            to_timestamp(col("tpep_dropoff_datetime"))
        )

    return df


def print_dataset_summary(df):
    print("Schema:")
    df.printSchema()

    print(f"Row count: {df.count()}")

    print("Sample records:")
    df.show(10, truncate=False)


if __name__ == "__main__":
    spark = create_spark_session()

    input_path = "data/raw/*.parquet"
    taxi_df = load_taxi_parquet(spark, input_path)

    print_dataset_summary(taxi_df)

    taxi_df.createOrReplaceTempView("taxi_trips")

    spark.stop()