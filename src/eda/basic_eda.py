from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    month,
    unix_timestamp,
    avg,
    count,
    sum as spark_sum,
    when,
    round as spark_round,
)


def create_spark_session(app_name: str = "NYC Taxi EDA") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    input_path = "data/raw/*.parquet"
    output_path = "outputs/eda"

    df = spark.read.parquet(input_path)

    df = (
        df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60,
        )
        .withColumn(
            "tip_percentage",
            when(col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100).otherwise(None),
        )
    )

    trip_volume_by_hour = (
        df.groupBy("pickup_hour")
        .agg(count("*").alias("trip_count"))
        .orderBy("pickup_hour")
    )

    fare_distance_summary = (
        df.agg(
            count("*").alias("total_trips"),
            avg("fare_amount").alias("avg_fare"),
            avg("trip_distance").alias("avg_trip_distance"),
            avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
            avg("tip_percentage").alias("avg_tip_percentage"),
        )
    )

    top_pickup_locations = (
        df.groupBy("PULocationID")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
        .limit(20)
    )

    payment_type_tip_behavior = (
        df.groupBy("payment_type")
        .agg(
            count("*").alias("trip_count"),
            avg("tip_percentage").alias("avg_tip_percentage"),
            avg("total_amount").alias("avg_total_amount"),
        )
        .orderBy("payment_type")
    )

    trip_volume_by_hour.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_path}/trip_volume_by_hour"
    )

    fare_distance_summary.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_path}/fare_distance_summary"
    )

    top_pickup_locations.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_path}/top_pickup_locations"
    )

    payment_type_tip_behavior.coalesce(1).write.mode("overwrite").option("header", True).csv(
        f"{output_path}/payment_type_tip_behavior"
    )

    print("EDA completed. Outputs written to outputs/eda/")

    spark.stop()


if __name__ == "__main__":
    main()