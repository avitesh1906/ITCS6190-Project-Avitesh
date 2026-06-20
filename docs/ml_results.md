# MLlib Fare Prediction Results

## Objective

The MLlib component predicts NYC taxi trip `total_amount` using trip, time, location, and payment features.

## Target Variable

- `total_amount`

## Features Used

- `trip_distance`
- `passenger_count`
- `pickup_hour`
- `pickup_day_of_week`
- `trip_duration_minutes`
- `PULocationID`
- `DOLocationID`
- `payment_type`

## Models

Two Spark MLlib regression models are trained:

1. Linear Regression
2. Random Forest Regression

## Evaluation Metrics

The models are evaluated using:

- RMSE
- MAE
- R2

## Command

```powershell
python src/ml_pipeline.py data/raw/yellow_tripdata_2024-01.parquet outputs/ml