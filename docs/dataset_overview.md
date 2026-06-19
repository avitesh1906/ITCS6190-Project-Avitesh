# Dataset Overview

This project uses the NYC Taxi and Limousine Commission Yellow Taxi Trip Records dataset.

## Source

NYC TLC Trip Record Data:
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Format

The current workflow expects monthly Yellow Taxi Parquet files stored locally in `data/raw/`.

Example:

```text
data/raw/yellow_tripdata_2024-01.parquet
```

## Key Fields Used

- `tpep_pickup_datetime` and `tpep_dropoff_datetime` for trip timing.
- `trip_distance` for distance-based analysis.
- `PULocationID` and `DOLocationID` for pickup/dropoff location analysis.
- `fare_amount`, `tip_amount`, and `total_amount` for revenue and tipping analysis.
- `payment_type` for payment-based behavior comparisons.

## Repository Policy

Raw Parquet files are not committed to the repository. Aggregated CSV outputs generated under `outputs/eda/` are also ignored so the repository stays lightweight.
