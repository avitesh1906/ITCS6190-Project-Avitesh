# ML pipeline placeholder
import csv
import os
import sys

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    unix_timestamp,
    when,
)

DEFAULT_INPUT_PATH = "data/raw/yellow_tripdata_2024-01.parquet"
DEFAULT_OUTPUT_PATH = "outputs/ml"


def create_spark_session(
    app_name: str = "NYC Taxi MLlib Fare Prediction",
) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def load_data(spark: SparkSession, input_path: str):
    return spark.read.parquet(input_path)


def prepare_features(df):
    """
    Prepare ML-ready features for fare prediction.

    Target variable:
    - total_amount

    Features:
    - trip_distance
    - passenger_count
    - pickup_hour
    - pickup_day_of_week
    - trip_duration_minutes
    - PULocationID
    - DOLocationID
    - payment_type
    """
    prepared_df = (
        df.withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
        .withColumn(
            "trip_duration_minutes",
            (
                unix_timestamp(col("tpep_dropoff_datetime"))
                - unix_timestamp(col("tpep_pickup_datetime"))
            )
            / 60,
        )
        .select(
            "trip_distance",
            "passenger_count",
            "pickup_hour",
            "pickup_day_of_week",
            "trip_duration_minutes",
            "PULocationID",
            "DOLocationID",
            "payment_type",
            "total_amount",
        )
    )

    # Basic data quality filters to remove invalid or extreme records.
    prepared_df = (
        prepared_df.filter(col("total_amount").isNotNull())
        .filter(col("trip_distance").isNotNull())
        .filter(col("passenger_count").isNotNull())
        .filter(col("trip_duration_minutes").isNotNull())
        .filter(col("pickup_hour").isNotNull())
        .filter(col("pickup_day_of_week").isNotNull())
        .filter(col("PULocationID").isNotNull())
        .filter(col("DOLocationID").isNotNull())
        .filter(col("payment_type").isNotNull())
        .filter(col("total_amount") > 0)
        .filter(col("total_amount") < 300)
        .filter(col("trip_distance") > 0)
        .filter(col("trip_distance") < 100)
        .filter(col("trip_duration_minutes") > 0)
        .filter(col("trip_duration_minutes") < 240)
        .filter(col("passenger_count") >= 0)
        .filter(col("passenger_count") <= 8)
    )

    return prepared_df


def train_models(training_df, test_df):
    feature_columns = [
        "trip_distance",
        "passenger_count",
        "pickup_hour",
        "pickup_day_of_week",
        "trip_duration_minutes",
        "PULocationID",
        "DOLocationID",
        "payment_type",
    ]

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features",
        handleInvalid="skip",
    )

    models = {
        "linear_regression": LinearRegression(
            featuresCol="features",
            labelCol="total_amount",
            predictionCol="prediction",
            maxIter=20,
            regParam=0.1,
            elasticNetParam=0.0,
        ),
        "random_forest_regression": RandomForestRegressor(
            featuresCol="features",
            labelCol="total_amount",
            predictionCol="prediction",
            numTrees=30,
            maxDepth=8,
            seed=42,
        ),
    }

    results = []

    for model_name, model in models.items():
        pipeline = Pipeline(stages=[assembler, model])
        fitted_model = pipeline.fit(training_df)
        predictions = fitted_model.transform(test_df)

        metrics = evaluate_model(predictions)
        metrics["model_name"] = model_name

        results.append(metrics)

    return results


def evaluate_model(predictions):
    evaluator_rmse = RegressionEvaluator(
        labelCol="total_amount",
        predictionCol="prediction",
        metricName="rmse",
    )

    evaluator_mae = RegressionEvaluator(
        labelCol="total_amount",
        predictionCol="prediction",
        metricName="mae",
    )

    evaluator_r2 = RegressionEvaluator(
        labelCol="total_amount",
        predictionCol="prediction",
        metricName="r2",
    )

    return {
        "rmse": round(evaluator_rmse.evaluate(predictions), 4),
        "mae": round(evaluator_mae.evaluate(predictions), 4),
        "r2": round(evaluator_r2.evaluate(predictions), 4),
    }


def write_metrics(results, output_path: str):
    os.makedirs(output_path, exist_ok=True)

    output_file = os.path.join(output_path, "fare_prediction_metrics.csv")

    columns = ["model_name", "rmse", "mae", "r2"]

    with open(output_file, mode="w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns)
        writer.writeheader()

        for result in results:
            writer.writerow(
                {
                    "model_name": result["model_name"],
                    "rmse": result["rmse"],
                    "mae": result["mae"],
                    "r2": result["r2"],
                }
            )

    print(f"Wrote ML metrics to: {output_file}")


def main():
    input_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT_PATH
    output_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_OUTPUT_PATH

    print(f"Reading taxi data from: {input_path}")
    print(f"Writing ML outputs to: {output_path}")

    spark = create_spark_session()

    raw_df = load_data(spark, input_path)
    prepared_df = prepare_features(raw_df)

    total_rows = prepared_df.count()
    print(f"ML-ready row count: {total_rows}")

    if total_rows == 0:
        raise ValueError("No rows available after ML feature preparation filters.")

    training_df, test_df = prepared_df.randomSplit([0.8, 0.2], seed=42)

    training_count = training_df.count()
    test_count = test_df.count()

    print(f"Training rows: {training_count}")
    print(f"Test rows: {test_count}")

    results = train_models(training_df, test_df)

    print("Model evaluation metrics:")
    for result in results:
        print(
            f"{result['model_name']}: "
            f"RMSE={result['rmse']}, "
            f"MAE={result['mae']}, "
            f"R2={result['r2']}"
        )

    write_metrics(results, output_path)

    spark.stop()


if __name__ == "__main__":
    main()
