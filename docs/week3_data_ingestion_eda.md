# Week 3 Progress Report: Data Ingestion and EDA

## Project

Analyzing NYC Yellow Taxi Trip Data with Apache Spark.

## Progress Completed

This week focused on making the data ingestion and exploratory analysis milestone reproducible and easier to review.

- Confirmed the NYC TLC Yellow Taxi Trip Records Parquet files as the primary dataset.
- Documented that raw monthly Parquet files should be downloaded locally into `data/raw/` and excluded from GitHub.
- Added a Spark ingestion script that reads local Yellow Taxi Parquet files and normalizes pickup/dropoff timestamp fields.
- Added schema inspection, row-count validation, and sample-record display for ingestion verification.
- Added an EDA workflow that writes aggregate CSV outputs under `outputs/eda/`.
- Expanded the EDA beyond basic counts by adding quality checks, temporal revenue patterns, fare and distance distributions, airport-trip comparisons, tip behavior bands, and outlier profiles.

## EDA Outputs

The EDA script now produces the following output folders:

- `trip_volume_by_hour`: trip counts, average total amount, and average tip percentage by pickup hour.
- `fare_distance_summary`: overall trip, fare, distance, duration, tip, speed, and percentile summary metrics.
- `top_pickup_locations`: highest-revenue pickup zones with trip counts, tip behavior, and dropoff diversity.
- `payment_type_tip_behavior`: tipping and total amount behavior grouped by payment type.
- `data_quality_summary`: raw versus valid trip counts and invalid-record reasons.
- `day_hour_revenue`: day-of-week and hour-level demand/revenue patterns.
- `distance_bucket_metrics`: fare, total amount, tip, and speed metrics by trip-distance bucket.
- `airport_trip_behavior`: comparison of airport-related and non-airport trips using JFK and LaGuardia TLC location IDs.
- `fare_outlier_profile`: median, p90, p95, and p99 fare/distance/duration indicators.
- `tip_band_by_payment_type`: payment-type distribution across no-tip and percentage-tip bands.

## Validation Plan

The ingestion and EDA scripts should be run first against one monthly Yellow Taxi Parquet file, then two or more months after schema consistency is confirmed.

Suggested local workflow:

```bash
python src/ingestion/load_taxi_data.py
python src/eda/basic_eda.py
```

Expected result:

- The ingestion script prints the schema, row count, and sample records.
- The EDA script writes CSV output directories under `outputs/eda/`.
- Raw input files and generated outputs remain uncommitted because they are ignored by `.gitignore`.

## Blockers and Risks

- The full TLC dataset is large, so early work should use one or two monthly Parquet files before scaling.
- Local Spark memory limits may require smaller batches, fewer months, or cluster execution.
- Monthly Yellow Taxi schemas need validation before combining several files.
- The source dataset is historical/static, so the streaming component will be simulated with Spark Structured Streaming by processing newly landed Parquet files as micro-batches.

## Next Steps

- Run the ingestion and expanded EDA scripts against at least one monthly Yellow Taxi Parquet file.
- Save reviewed aggregate outputs under `outputs/eda/` locally for analysis screenshots or summary tables.
- Add the first version of the landing-folder Structured Streaming simulation.
- Use the EDA outputs to choose stronger SQL, ML, and streaming analysis questions for the next milestone.
