# Results Summary

## Overview

This project analyzes NYC TLC Yellow Taxi trip data using Apache Spark. The current analysis uses one monthly Yellow Taxi Parquet file as the local validation dataset.

## EDA Outputs

The EDA pipeline generates the following analytical outputs:

- trip volume by pickup hour
- fare and distance summary
- top pickup locations by revenue
- payment-type-based tip behavior
- data quality summary
- day/hour revenue trends
- distance bucket metrics
- airport vs non-airport trip behavior
- fare outlier profile
- tip bands by payment type

## Visualizations

The visualization script creates charts from the generated EDA CSV outputs:

- trip volume by pickup hour
- top pickup locations by revenue
- trip count by distance bucket
- trip count by tip band
- revenue trend by pickup hour

Charts are generated locally under:

```text
outputs/charts/