# Methodology

## Project Objective

This project builds a big data analytics pipeline using Apache Spark to analyze NYC TLC Yellow Taxi trip records. The goal is to demonstrate an end-to-end Spark workflow that includes batch ingestion, exploratory data analysis, simulated real-time streaming, machine learning with MLlib, and reproducible outputs.

## Dataset

The project uses the NYC TLC Yellow Taxi Trip Records dataset. The data is publicly available as monthly Parquet files from the NYC Taxi & Limousine Commission.

For local validation, the project uses:

```text
yellow_tripdata_2024-01.parquet
```

Raw Parquet files are stored locally under:

```text
data/raw/
```

Raw data is excluded from GitHub because of file size and reproducibility policy.

## Batch Ingestion

The ingestion pipeline is implemented in:

```text
src/ingestion/load_taxi_data.py
```

The ingestion script:

* starts a Spark session
* reads the Yellow Taxi Parquet file into a Spark DataFrame
* prints the dataset schema
* calculates the row count
* displays sample records
* registers a temporary Spark SQL view named `taxi_trips`

The script supports both a default input path and a command-line input path.

## Exploratory Data Analysis

The EDA pipeline is implemented in:

```text
src/eda/basic_eda.py
```

The EDA script uses Spark DataFrame transformations and aggregations to derive analytical fields and generate summary outputs.

Derived fields include:

* pickup hour
* pickup day of week
* pickup month
* trip duration in minutes
* tip percentage
* distance bucket
* airport trip indicator
* fare outlier flag

EDA outputs include:

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

The aggregated outputs are written as CSV files under:

```text
outputs/eda/
```

Because the final EDA outputs are small aggregated tables, the project writes them using Python's standard CSV module. This avoids local Windows Hadoop file writer issues while keeping Spark responsible for the data processing and aggregations.

## Structured Streaming

The streaming pipeline is implemented in:

```text
src/streaming/file_streaming_simulation.py
```

The NYC TLC dataset is historical, so the project simulates real-time taxi trip events using Spark Structured Streaming.

The streaming job uses Spark's `rate` source to generate continuous event rows. Taxi-like fields are derived from the generated stream, including:

* pickup hour
* trip distance
* passenger count
* fare amount
* tip amount
* total amount
* pickup location ID
* dropoff location ID
* payment type
* tip percentage

The streaming job processes micro-batches and calculates:

* trip count
* average fare amount
* average trip distance
* average tip percentage
* average total amount

Results are printed to the console for each micro-batch.

## Machine Learning with MLlib

The MLlib pipeline is implemented in:

```text
src/ml_pipeline.py
```

The machine learning objective is to predict taxi trip `total_amount`.

The target variable is:

```text
total_amount
```

Features used include:

* trip distance
* passenger count
* pickup hour
* pickup day of week
* trip duration in minutes
* pickup location ID
* dropoff location ID
* payment type

The pipeline trains two Spark MLlib regression models:

1. Linear Regression
2. Random Forest Regression

The models are evaluated using:

* RMSE
* MAE
* R2

Evaluation metrics are written to:

```text
outputs/ml/fare_prediction_metrics.csv
```

## Visualization

The visualization script is implemented in:

```text
src/visualization/create_charts.py
```

The script reads aggregated EDA CSV outputs and creates charts under:

```text
outputs/charts/
```

Generated charts include:

* trip volume by pickup hour
* top pickup locations by revenue
* trip count by distance bucket
* trip count by tip band
* revenue trend by pickup hour

Generated charts are excluded from GitHub because they are reproducible outputs.

## Reproducibility

The project is designed so each stage can be run from the command line. The final pipeline will also be connected through `run.sh` and/or `make run` for one-command execution.

The main reproducibility instructions are documented in:

```text
docs/reproduction_guide.md
```
