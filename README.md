# ITCS 6190 - Cloud Computing for Data Analysis

## Course Project: Analyzing NYC Taxi Trip Data with Apache Spark

This project analyzes NYC TLC Yellow Taxi Trip Records using Apache Spark for batch ingestion, exploratory data analysis, SQL-style analytics, streaming simulation, and machine-learning experiments.

## Current Milestone

The current milestone is Data Ingestion and EDA. See [docs/week3_data_ingestion_eda.md](docs/week3_data_ingestion_eda.md) for the progress report, validation plan, blockers, and next steps.

## Local Data Setup

Download one or more Yellow Taxi monthly Parquet files from the NYC TLC trip records page and place them under:

```text
data/raw/
```

Raw Parquet files and generated EDA outputs are intentionally ignored by Git.

## Run Ingestion and EDA

```bash
python src/ingestion/load_taxi_data.py
python src/eda/basic_eda.py
```

EDA outputs are written locally under:

```text
outputs/eda/
```
