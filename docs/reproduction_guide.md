# Reproduction Guide

This guide explains how to reproduce the NYC Taxi Trip Data with Apache Spark project locally.

The pipeline currently supports:

* Data ingestion validation
* Expanded EDA
* Structured Streaming simulation
* MLlib fare prediction
* EDA visualization generation

Raw data and generated outputs are intentionally excluded from GitHub.

---

## 1. Install Dependencies

From the project root, install Python dependencies:

```bash
pip install -r requirements.txt
```

---

## 2. Download Data

Download one or more monthly Yellow Taxi Parquet files from the NYC TLC Trip Record Data page:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Place the downloaded files under:

```text
data/raw/
```

Example local file:

```text
data/raw/yellow_tripdata_2024-01.parquet
```

Raw Parquet files should remain local and should not be committed to GitHub.

---

## 3. Validate Data Ingestion

Run the ingestion script using the default January 2024 Yellow Taxi file:

```powershell
python src/ingestion/load_taxi_data.py
```

Or run with an explicit input path:

```powershell
python src/ingestion/load_taxi_data.py data/raw/yellow_tripdata_2024-01.parquet
```

The ingestion script:

* loads the Parquet file using Spark
* prints the schema
* calculates the row count
* displays sample records
* registers a temporary Spark SQL view named `taxi_trips`

---

## 4. Run EDA

Run the EDA script using the default input and output paths:

```powershell
python src/eda/basic_eda.py
```

Or run with explicit input and output paths:

```powershell
python src/eda/basic_eda.py data/raw/yellow_tripdata_2024-01.parquet outputs/eda
```

The EDA script creates derived fields such as:

* pickup hour
* pickup day of week
* pickup month
* trip duration in minutes
* tip percentage
* distance bucket
* airport trip indicator
* fare outlier flag

The script writes aggregated CSV outputs under:

```text
outputs/eda/
```

Expected EDA outputs include:

* `trip_volume_by_hour.csv`
* `fare_distance_summary.csv`
* `top_pickup_locations.csv`
* `payment_type_tip_behavior.csv`
* `data_quality_summary.csv`
* `day_hour_revenue.csv`
* `distance_bucket_metrics.csv`
* `airport_trip_behavior.csv`
* `fare_outlier_profile.csv`
* `tip_band_by_payment_type.csv`

---

## 5. Run Structured Streaming Simulation

The NYC TLC dataset is historical/static, so the project simulates real-time taxi events using Spark Structured Streaming.

Run the streaming simulation:

```powershell
python src/streaming/file_streaming_simulation.py 5
```

The numeric argument controls simulated rows per second.

The streaming job:

* uses Spark's `rate` source
* derives taxi-like streaming fields
* processes records in micro-batches
* computes trip count, average fare, average distance, average tip percentage, and average total amount
* prints micro-batch results to the console

Stop the streaming job with:

```text
Ctrl + C
```

### Windows Note

On Windows, Spark Structured Streaming may require Hadoop native utilities such as `winutils.exe` and `hadoop.dll`.

Expected local setup:

```text
C:\hadoop\bin\winutils.exe
C:\hadoop\bin\hadoop.dll
```

For the current PowerShell session, you can set:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "C:\hadoop\bin;$env:Path"
```

Then verify:

```powershell
where.exe winutils.exe
where.exe hadoop.dll
```

---

## 6. Run MLlib Fare Prediction

Run the MLlib pipeline:

```powershell
python src/ml_pipeline.py data/raw/yellow_tripdata_2024-01.parquet outputs/ml
```

The ML pipeline predicts:

```text
total_amount
```

Features used include:

* `trip_distance`
* `passenger_count`
* `pickup_hour`
* `pickup_day_of_week`
* `trip_duration_minutes`
* `PULocationID`
* `DOLocationID`
* `payment_type`

The pipeline trains:

* Linear Regression
* Random Forest Regression

Evaluation metrics include:

* RMSE
* MAE
* R2

ML metrics are written to:

```text
outputs/ml/fare_prediction_metrics.csv
```

---

## 7. Generate Visualizations

After running the EDA script, generate charts with:

```powershell
python src/visualization/create_charts.py outputs/eda outputs/charts
```

Charts are written under:

```text
outputs/charts/
```

Expected charts include:

* `trip_volume_by_hour.png`
* `top_pickup_locations_by_revenue.png`
* `distance_bucket_trip_count.png`
* `tip_band_trip_count.png`
* `revenue_by_pickup_hour.png`

---

## 8. Review Outputs

Use the generated outputs to inspect:

* data quality issues and invalid trip records
* demand and revenue by pickup hour and day of week
* fare, distance, duration, and tip distributions
* top pickup locations by revenue
* airport-related trip behavior
* payment-type tipping patterns
* ML model evaluation metrics

Generated output folders should remain local and should not be committed to GitHub.

Common generated folders include:

```text
outputs/eda/
outputs/ml/
outputs/charts/
outputs/checkpoints/
```

---

## 9. Repository Data Policy

The following should not be committed:

* raw Parquet files
* generated CSV outputs
* generated chart images
* Spark checkpoint folders
* local temporary files

Only source code, documentation, and reproducibility instructions should be committed.

## One-Command Pipeline Execution

### Windows PowerShell

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1