# Final Report: NYC Taxi Trip Data Analysis with Apache Spark

## 1. Project Overview

This project implements a big data analytics pipeline using Apache Spark to analyze NYC TLC Yellow Taxi trip records. The pipeline demonstrates batch ingestion, exploratory data analysis, simulated real-time streaming, machine learning with Spark MLlib, and visualization.

The project uses the NYC TLC Yellow Taxi Trip Records dataset, which is publicly available in monthly Parquet format.

## 2. Analytical Questions

The project is designed around the following questions:

1. How do taxi trip volumes, fares, and revenue vary by pickup hour and day of week?
2. Which pickup locations generate the highest revenue?
3. How do trip distance, duration, and payment type relate to fare and tip behavior?
4. How do airport-related trips differ from non-airport trips?
5. Can Spark MLlib predict taxi trip total amount using trip, time, location, and payment features?
6. How can historical taxi data be adapted into a simulated real-time streaming workflow?

## 3. Dataset

Dataset:

* NYC TLC Yellow Taxi Trip Records

Source:

* NYC Taxi & Limousine Commission Trip Record Data

Local validation file:

```text
yellow_tripdata_2024-01.parquet
```

The January 2024 Yellow Taxi dataset contained:

```text
2,964,624 rows
```

Raw Parquet files are stored locally under:

```text
data/raw/
```

Raw data is not committed to GitHub.

## 4. Pipeline Architecture

The project pipeline includes the following stages:

1. Data ingestion using Spark DataFrames
2. Exploratory data analysis using Spark transformations and aggregations
3. Structured Streaming simulation using Spark's rate source
4. MLlib fare prediction using regression models
5. Visualization generation from EDA outputs
6. Documentation and reproducibility support

## 5. Data Ingestion

The ingestion pipeline is implemented in:

```text
src/ingestion/load_taxi_data.py
```

The script reads the Yellow Taxi Parquet file into a Spark DataFrame, prints the schema, calculates the row count, displays sample records, and registers a temporary Spark SQL view named `taxi_trips`.

The ingestion script supports a default file path and command-line input paths.

## 6. Exploratory Data Analysis

The EDA pipeline is implemented in:

```text
src/eda/basic_eda.py
```

The EDA script creates derived fields including:

* pickup hour
* pickup day of week
* pickup month
* trip duration in minutes
* tip percentage
* distance bucket
* airport trip indicator
* fare outlier flag

Generated EDA outputs include:

* trip volume by pickup hour
* fare and distance summary
* top pickup locations by revenue
* payment-type-based tip behavior
* data quality summary
* day/hour revenue trends
* distance bucket metrics
* airport vs non-airport trip behavior
* fare outlier profile
* tip bands by payment type

The outputs are written locally under:

```text
outputs/eda/
```

## 7. Structured Streaming

The streaming component is implemented in:

```text
src/streaming/file_streaming_simulation.py
```

Because the NYC TLC dataset is historical/static, the project simulates real-time taxi events using Spark Structured Streaming's `rate` source.

The streaming job derives taxi-like fields from generated streaming rows, including:

* trip distance
* passenger count
* fare amount
* tip amount
* total amount
* pickup location ID
* dropoff location ID
* payment type
* tip percentage

The streaming job processes micro-batches and computes:

* trip count
* average fare amount
* average trip distance
* average tip percentage
* average total amount

Streaming results are printed to the console.

## 8. Machine Learning with Spark MLlib

The MLlib component is implemented in:

```text
src/ml_pipeline.py
```

The machine learning task is regression.

Target variable:

```text
total_amount
```

Features:

* trip distance
* passenger count
* pickup hour
* pickup day of week
* trip duration in minutes
* pickup location ID
* dropoff location ID
* payment type

Models trained:

1. Linear Regression
2. Random Forest Regression

Evaluation metrics:

| Model                    |   RMSE |    MAE |     R2 |
| ------------------------ | -----: | -----: | -----: |
| Linear Regression        | 7.0172 | 3.2655 | 0.8926 |
| Random Forest Regression | 6.3212 | 2.5856 | 0.9129 |

The Random Forest Regression model performed better, with lower RMSE and MAE and higher R2.

## 9. Visualizations

The visualization script is implemented in:

```text
src/visualization/create_charts.py
```

The script reads EDA CSV outputs and generates charts under:

```text
outputs/charts/
```

Generated visualizations include:

* trip volume by pickup hour
* top pickup locations by revenue
* trip count by distance bucket
* trip count by tip band
* revenue trend by pickup hour

## 10. Key Findings

The January 2024 Yellow Taxi dataset supports analysis of demand, fare behavior, pickup patterns, tipping behavior, and model-based fare prediction.

Key findings from the current validation run include:

* The dataset is large enough for distributed-style Spark processing, with nearly 3 million records in one month.
* Trip behavior can be meaningfully grouped by pickup hour, distance bucket, payment type, and location ID.
* Random Forest Regression performed better than Linear Regression for predicting total trip amount.
* Simulated streaming provides a practical way to demonstrate real-time processing concepts on historical taxi data.
* Local Windows execution required practical adjustments for Spark/Hadoop file-writing and streaming behavior.

## 11. Limitations

Important limitations include:

* The current validation uses only one monthly Parquet file.
* The streaming data is simulated rather than live taxi event data.
* Geographic analysis uses numeric location IDs rather than readable borough or zone names.
* The ML model does not include weather, traffic, holidays, or event data.
* Hyperparameter tuning and model persistence are not included.
* Generated outputs are excluded from GitHub and must be reproduced locally.

More details are documented in:

```text
docs/limitations.md
```

## 12. Reproducibility

The project supports one-command Windows execution using:

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1
```

The batch pipeline runs:

1. ingestion validation
2. EDA
3. MLlib fare prediction
4. visualization generation

Structured Streaming is run separately because it is a long-running process:

```powershell
python src/streaming/file_streaming_simulation.py 5
```

Full reproduction instructions are available in:

```text
docs/reproduction_guide.md
```

## 13. Conclusion

This project demonstrates an end-to-end Apache Spark analytics workflow on a real public dataset. It integrates Spark Structured APIs, simulated Structured Streaming, Spark MLlib, EDA, visualization, documentation, and one-command reproducibility.

The final pipeline satisfies the major project goals by showing how Spark can ingest, process, analyze, stream, model, and communicate insights from large-scale taxi trip data.
