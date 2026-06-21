import csv
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    unix_timestamp,
    avg,
    count,
    countDistinct,
    dayofweek,
    hour,
    max as spark_max,
    min as spark_min,
    month,
    percentile_approx,
    sum as spark_sum,
    stddev,
    when,
    col,
    round as spark_round,
)

DEFAULT_INPUT_PATH = "data/raw/yellow_tripdata_2024-01.parquet"
DEFAULT_OUTPUT_PATH = "outputs/eda"


def create_spark_session(app_name: str = "NYC Taxi EDA") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def add_eda_columns(df):
    return (
        df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            (
                unix_timestamp(col("tpep_dropoff_datetime"))
                - unix_timestamp(col("tpep_pickup_datetime"))
            )
            / 60,
        )
        .withColumn(
            "tip_percentage",
            when(
                col("fare_amount") > 0, (col("tip_amount") / col("fare_amount")) * 100
            ).otherwise(None),
        )
        .withColumn(
            "avg_mph",
            when(
                (col("trip_duration_minutes") > 0) & (col("trip_distance") >= 0),
                col("trip_distance") / (col("trip_duration_minutes") / 60),
            ).otherwise(None),
        )
        .withColumn(
            "is_valid_trip",
            col("trip_distance").isNotNull()
            & col("fare_amount").isNotNull()
            & col("total_amount").isNotNull()
            & col("trip_duration_minutes").isNotNull()
            & (col("trip_distance") > 0)
            & (col("fare_amount") > 0)
            & (col("total_amount") > 0)
            & (col("trip_duration_minutes") > 0)
            & (col("trip_duration_minutes") <= 24 * 60),
        )
        .withColumn(
            "distance_bucket",
            when(col("trip_distance") < 1, "00_under_1_mile")
            .when(col("trip_distance") < 3, "01_1_to_3_miles")
            .when(col("trip_distance") < 7, "02_3_to_7_miles")
            .when(col("trip_distance") < 15, "03_7_to_15_miles")
            .otherwise("04_15_plus_miles"),
        )
        .withColumn(
            "airport_trip_type",
            when(
                (col("PULocationID").isin(132, 138))
                | (col("DOLocationID").isin(132, 138)),
                "airport_related",
            ).otherwise("non_airport"),
        )
        .withColumn(
            "tip_band",
            when(col("tip_amount") <= 0, "00_no_tip")
            .when(col("tip_percentage") < 10, "01_under_10_pct")
            .when(col("tip_percentage") < 20, "02_10_to_20_pct")
            .when(col("tip_percentage") < 30, "03_20_to_30_pct")
            .otherwise("04_30_plus_pct"),
        )
    )


def build_eda_outputs(df):
    valid_df = df.filter(col("is_valid_trip"))

    trip_volume_by_hour = (
        valid_df.groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
        )
        .orderBy("pickup_hour")
    )

    fare_distance_summary = valid_df.agg(
        count("*").alias("total_trips"),
        spark_round(avg("fare_amount"), 2).alias("avg_fare"),
        spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        spark_round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
        spark_round(avg("avg_mph"), 2).alias("avg_mph"),
        spark_round(stddev("total_amount"), 2).alias("stddev_total_amount"),
        spark_round(percentile_approx("total_amount", 0.5), 2).alias(
            "median_total_amount"
        ),
        spark_round(percentile_approx("trip_distance", 0.95), 2).alias(
            "p95_trip_distance"
        ),
    )

    top_pickup_locations = (
        valid_df.groupBy("PULocationID")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
            countDistinct("DOLocationID").alias("distinct_dropoff_locations"),
        )
        .orderBy(col("total_revenue").desc())
        .limit(20)
    )

    payment_type_tip_behavior = (
        valid_df.groupBy("payment_type")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
            spark_round(spark_sum("tip_amount"), 2).alias("total_tips"),
        )
        .orderBy("payment_type")
    )

    data_quality_summary = df.agg(
        count("*").alias("raw_trips"),
        spark_sum(when(col("is_valid_trip"), 1).otherwise(0)).alias("valid_trips"),
        spark_sum(when(~col("is_valid_trip"), 1).otherwise(0)).alias("invalid_trips"),
        spark_sum(when(col("trip_distance") <= 0, 1).otherwise(0)).alias(
            "non_positive_distance_trips"
        ),
        spark_sum(when(col("fare_amount") <= 0, 1).otherwise(0)).alias(
            "non_positive_fare_trips"
        ),
        spark_sum(when(col("trip_duration_minutes") <= 0, 1).otherwise(0)).alias(
            "non_positive_duration_trips"
        ),
        spark_sum(when(col("trip_duration_minutes") > 24 * 60, 1).otherwise(0)).alias(
            "over_24_hour_trips"
        ),
        spark_min("tpep_pickup_datetime").alias("min_pickup_datetime"),
        spark_max("tpep_pickup_datetime").alias("max_pickup_datetime"),
    )

    day_hour_revenue = (
        valid_df.groupBy("pickup_day_of_week", "pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        )
        .orderBy("pickup_day_of_week", "pickup_hour")
    )

    distance_bucket_metrics = (
        valid_df.groupBy("distance_bucket")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("fare_amount"), 2).alias("avg_fare"),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
            spark_round(avg("avg_mph"), 2).alias("avg_mph"),
        )
        .orderBy("distance_bucket")
    )

    airport_trip_behavior = (
        valid_df.groupBy("airport_trip_type")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            spark_round(avg("trip_duration_minutes"), 2).alias(
                "avg_trip_duration_minutes"
            ),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
            spark_round(avg("tip_percentage"), 2).alias("avg_tip_percentage"),
        )
        .orderBy("airport_trip_type")
    )

    fare_outlier_profile = valid_df.select(
        spark_round(percentile_approx("total_amount", 0.5), 2).alias(
            "median_total_amount"
        ),
        spark_round(percentile_approx("total_amount", 0.9), 2).alias(
            "p90_total_amount"
        ),
        spark_round(percentile_approx("total_amount", 0.95), 2).alias(
            "p95_total_amount"
        ),
        spark_round(percentile_approx("total_amount", 0.99), 2).alias(
            "p99_total_amount"
        ),
        spark_round(percentile_approx("trip_distance", 0.99), 2).alias(
            "p99_trip_distance"
        ),
        spark_round(percentile_approx("trip_duration_minutes", 0.99), 2).alias(
            "p99_trip_duration_minutes"
        ),
    )

    tip_band_by_payment_type = (
        valid_df.groupBy("payment_type", "tip_band")
        .agg(
            count("*").alias("trip_count"),
            spark_round(avg("total_amount"), 2).alias("avg_total_amount"),
        )
        .withColumn(
            "share_of_payment_type",
            spark_round(
                col("trip_count")
                / spark_sum("trip_count").over(Window.partitionBy("payment_type")),
                4,
            ),
        )
        .orderBy("payment_type", "tip_band")
    )

    return {
        "trip_volume_by_hour": trip_volume_by_hour,
        "fare_distance_summary": fare_distance_summary,
        "top_pickup_locations": top_pickup_locations,
        "payment_type_tip_behavior": payment_type_tip_behavior,
        "data_quality_summary": data_quality_summary,
        "day_hour_revenue": day_hour_revenue,
        "distance_bucket_metrics": distance_bucket_metrics,
        "airport_trip_behavior": airport_trip_behavior,
        "fare_outlier_profile": fare_outlier_profile,
        "tip_band_by_payment_type": tip_band_by_payment_type,
    }


def write_eda_outputs(outputs, output_path):
    os.makedirs(output_path, exist_ok=True)

    for name, output_df in outputs.items():
        file_path = os.path.join(output_path, f"{name}.csv")

        rows = output_df.collect()
        columns = output_df.columns

        with open(file_path, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(columns)

            for row in rows:
                writer.writerow([row[column] for column in columns])

        print(f"Wrote output: {file_path}")


def main():
    spark = create_spark_session()

    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_PATH
    output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_PATH

    print(f"Reading taxi data from: {input_path}")
    print(f"Writing EDA outputs to: {output_path}")

    df = spark.read.parquet(input_path)
    eda_df = add_eda_columns(df)
    outputs = build_eda_outputs(eda_df)
    write_eda_outputs(outputs, output_path)

    print("EDA completed. Outputs written to outputs/eda/")

    spark.stop()


if __name__ == "__main__":
    main()
