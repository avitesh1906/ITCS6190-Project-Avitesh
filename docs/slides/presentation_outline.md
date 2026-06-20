# Presentation Outline: NYC Taxi Trip Data Analysis with Apache Spark

## Slide 1: Title

**NYC Taxi Trip Data Analysis with Apache Spark**

Team:
- Avitesh Kesharwani
- Vartika Gupta

Course:
- ITCS 6190 Cloud Computing for Data Analysis

## Slide 2: Project Goal

Build an end-to-end big data analytics pipeline using Apache Spark to analyze NYC TLC Yellow Taxi trip records.

Pipeline components:

- Spark DataFrame ingestion
- Exploratory Data Analysis
- Structured Streaming simulation
- Spark MLlib fare prediction
- Visualization and documentation
- One-command reproducible execution

## Slide 3: Dataset

Dataset:

- NYC TLC Yellow Taxi Trip Records
- Format: monthly Parquet files
- Local validation file: `yellow_tripdata_2024-01.parquet`
- Row count: `2,964,624`

Important fields:

- pickup/dropoff datetime
- passenger count
- trip distance
- pickup/dropoff location IDs
- fare amount
- tip amount
- total amount
- payment type

## Slide 4: Architecture

Pipeline stages:

1. Data ingestion
2. EDA and derived features
3. Structured Streaming simulation
4. MLlib fare prediction
5. Visualization generation
6. Final documentation and reproducibility

Main scripts:

- `src/ingestion/load_taxi_data.py`
- `src/eda/basic_eda.py`
- `src/streaming/file_streaming_simulation.py`
- `src/ml_pipeline.py`
- `src/visualization/create_charts.py`
- `run.ps1`

## Slide 5: Data Ingestion

The ingestion script:

- loads Parquet data into Spark DataFrame
- prints schema
- validates row count
- displays sample records
- registers temporary Spark SQL view `taxi_trips`

Validation:

- January 2024 dataset loaded successfully
- Row count: `2,964,624`

## Slide 6: EDA

Derived fields:

- pickup hour
- pickup day of week
- pickup month
- trip duration
- tip percentage
- distance bucket
- airport trip indicator
- fare outlier flag

EDA outputs:

- trip volume by hour
- revenue by day/hour
- top pickup locations
- payment-type tip behavior
- fare outlier profile
- distance bucket metrics

## Slide 7: Structured Streaming

Because TLC data is historical, streaming is simulated using Spark's `rate` source.

Streaming job:

- generates simulated taxi events
- derives taxi-like fields
- processes micro-batches
- computes trip count, average fare, average distance, average tip percentage, and average total amount
- prints micro-batch results to console

## Slide 8: MLlib Fare Prediction

ML task:

- Regression model to predict `total_amount`

Features:

- trip distance
- passenger count
- pickup hour
- pickup day of week
- trip duration
- pickup location ID
- dropoff location ID
- payment type

Models:

- Linear Regression
- Random Forest Regression

## Slide 9: ML Results

| Model | RMSE | MAE | R2 |
|---|---:|---:|---:|
| Linear Regression | 7.0172 | 3.2655 | 0.8926 |
| Random Forest Regression | 6.3212 | 2.5856 | 0.9129 |

Conclusion:

- Random Forest performed better across all three metrics.
- Trip distance, duration, time, and location-based features provide useful prediction signal.

## Slide 10: Visualizations

Generated charts:

- trip volume by pickup hour
- top pickup locations by revenue
- trip count by distance bucket
- trip count by tip band
- revenue trend by pickup hour

Chart outputs are generated under:

`outputs/charts/`

## Slide 11: Reproducibility

Windows one-command pipeline:

```powershell
powershell -ExecutionPolicy Bypass -File .\run.ps1