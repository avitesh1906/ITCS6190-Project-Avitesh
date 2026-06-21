import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
    hour,
    round as spark_round,
    when,
)

DEFAULT_ROWS_PER_SECOND = 5


def create_spark_session(
    app_name: str = "NYC Taxi Structured Streaming Simulation",
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def build_simulated_taxi_stream(spark: SparkSession, rows_per_second: int):
    """
    Creates a simulated taxi trip event stream using Spark's rate source.

    The rate source generates streaming rows with:
    - timestamp
    - value

    Taxi-like fields are derived from the generated stream so the stream behaves
    like incoming NYC taxi trip events.
    """
    rate_df = (
        spark.readStream.format("rate").option("rowsPerSecond", rows_per_second).load()
    )

    taxi_stream_df = (
        rate_df.withColumn("pickup_hour", hour(col("timestamp")))
        .withColumn("trip_distance", ((col("value") % 20) + 1).cast("double"))
        .withColumn("passenger_count", ((col("value") % 4) + 1).cast("integer"))
        .withColumn("fare_amount", (col("trip_distance") * 3.25 + 5.00))
        .withColumn(
            "tip_amount",
            when(col("value") % 3 == 0, col("fare_amount") * 0.20).otherwise(
                col("fare_amount") * 0.12
            ),
        )
        .withColumn("total_amount", col("fare_amount") + col("tip_amount"))
        .withColumn("PULocationID", ((col("value") % 260) + 1).cast("integer"))
        .withColumn("DOLocationID", (((col("value") + 17) % 260) + 1).cast("integer"))
        .withColumn("payment_type", ((col("value") % 2) + 1).cast("integer"))
        .withColumn("tip_percentage", (col("tip_amount") / col("fare_amount")) * 100)
        .withColumn("processing_time", current_timestamp())
    )

    return taxi_stream_df


def process_micro_batch(batch_df, batch_id: int):
    """
    Processes each streaming micro-batch.

    This avoids Spark checkpoint/file-output issues on Windows while still using
    Structured Streaming for ingestion and Spark DataFrame operations for
    per-batch aggregations.
    """
    print(f"\n========== Micro-batch {batch_id} ==========")

    if batch_df.isEmpty():
        print("No records in this micro-batch.")
        return

    summary_df = (
        batch_df.groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare_amount"),
            spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
        )
        .orderBy("pickup_hour")
    )

    summary_df.show(truncate=False)


def main():
    rows_per_second = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROWS_PER_SECOND

    print(f"Rows per second: {rows_per_second}")

    spark = create_spark_session()

    taxi_stream_df = build_simulated_taxi_stream(spark, rows_per_second)

    query = (
        taxi_stream_df.writeStream.foreachBatch(process_micro_batch)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("Structured Streaming simulation started. Press Ctrl+C to stop.")

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping streaming query...")
        query.stop()

    spark.stop()


if __name__ == "__main__":
    main()
