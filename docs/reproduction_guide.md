# Reproduction Guide

## 1. Install Dependencies

```bash
pip install -r requirements.txt
```

## 2. Download Data

Download one or more monthly Yellow Taxi Parquet files from the NYC TLC Trip Record Data page:

https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Place the files in:

```text
data/raw/
```

## 3. Validate Ingestion

```bash
python src/ingestion/load_taxi_data.py
```

The script prints the Spark schema, row count, and sample records.

## 4. Run EDA

```bash
python src/eda/basic_eda.py
```

The script writes aggregated CSV output directories under:

```text
outputs/eda/
```

## 5. Review Outputs

Use the generated EDA tables to inspect:

- data quality issues and invalid trip records
- demand and revenue by pickup hour and day of week
- fare, distance, duration, and tip distributions
- top pickup locations by revenue
- airport-related trip behavior
- payment-type tipping patterns

Raw Parquet files and generated CSV outputs should stay local and uncommitted.
