# Limitations

## Dataset Scope

The current local validation uses one monthly Yellow Taxi Parquet file:

```text
yellow_tripdata_2024-01.parquet
```

This is sufficient to validate the Spark pipeline locally, but results may not represent long-term seasonal trends across multiple months or years.

## Local Execution Constraints

The project was developed and validated in local Spark mode on Windows. Some Spark/Hadoop file operations on Windows required workarounds because of Hadoop native library behavior.

Observed constraints included:

* Spark CSV writer issues during local EDA output generation
* Structured Streaming checkpoint behavior requiring local Hadoop utilities
* Need for `winutils.exe` and `hadoop.dll` under `C:\hadoop\bin`

To keep the project reproducible locally:

* EDA aggregated outputs are written using Python's standard CSV module
* Structured Streaming uses the Spark `rate` source for simulation
* Streaming results are printed to the console instead of being written to checkpointed file sinks

## Streaming Simulation

The original dataset is historical/static rather than real-time. Therefore, real-time streaming is simulated.

The final streaming implementation uses Spark's `rate` source to generate synthetic taxi-like trip events. This demonstrates Structured Streaming concepts, including:

* streaming source ingestion
* transformation
* micro-batch processing
* aggregation
* console output

However, it does not represent live TLC taxi events.

## Location Analysis

The current analysis uses `PULocationID` and `DOLocationID` directly. It does not yet join with the official NYC taxi zone lookup table to map zone IDs to boroughs or zone names.

As a result, geographic analysis is currently based on numeric location IDs rather than readable neighborhood or borough labels.

## External Factors

The current model and analysis do not include external factors such as:

* weather
* holidays
* public events
* traffic incidents
* subway disruptions
* airport delay data

These factors could improve demand, fare, and duration analysis but are outside the current project scope.

## ML Model Limitations

The MLlib fare prediction model is intended as a course-project demonstration of Spark MLlib, not a production fare prediction system.

Current limitations include:

* limited feature set
* no hyperparameter tuning
* no advanced categorical encoding beyond numeric location/payment IDs
* no model persistence step
* no comparison across multiple months
* possible bias from outlier filtering choices

The model predicts `total_amount`, which may be strongly influenced by fare rules, surcharges, tips, tolls, and fees.

## Output Artifacts

Generated outputs are not committed to GitHub. This includes:

* raw Parquet files
* EDA CSV outputs
* ML metrics CSV files
* chart images
* Spark checkpoint folders

This keeps the repository lightweight, but users must rerun the pipeline locally to regenerate outputs.

## Future Improvements

Potential improvements include:

* processing multiple months or a full year of data
* joining with taxi zone lookup data
* adding weather or event data
* adding model persistence
* adding hyperparameter tuning
* improving visualization quality
* running the pipeline in a cloud environment such as AWS EMR or Databricks
* replacing simulated streaming with a true message-source stream such as Kafka
