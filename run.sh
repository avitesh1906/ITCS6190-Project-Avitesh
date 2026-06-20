#!/usr/bin/env bash

set -e

INPUT_PATH=${1:-data/raw/yellow_tripdata_2024-01.parquet}
EDA_OUTPUT_PATH=${2:-outputs/eda}
ML_OUTPUT_PATH=${3:-outputs/ml}
CHART_OUTPUT_PATH=${4:-outputs/charts}

echo "========================================"
echo "NYC Taxi Spark Pipeline"
echo "========================================"
echo "Input path: ${INPUT_PATH}"
echo "EDA output path: ${EDA_OUTPUT_PATH}"
echo "ML output path: ${ML_OUTPUT_PATH}"
echo "Chart output path: ${CHART_OUTPUT_PATH}"
echo "========================================"

echo ""
echo "Step 1: Validate ingestion"
python src/ingestion/load_taxi_data.py "${INPUT_PATH}"

echo ""
echo "Step 2: Run EDA"
python src/eda/basic_eda.py "${INPUT_PATH}" "${EDA_OUTPUT_PATH}"

echo ""
echo "Step 3: Run MLlib fare prediction"
python src/ml_pipeline.py "${INPUT_PATH}" "${ML_OUTPUT_PATH}"

echo ""
echo "Step 4: Generate visualizations"
python src/visualization/create_charts.py "${EDA_OUTPUT_PATH}" "${CHART_OUTPUT_PATH}"

echo ""
echo "Pipeline completed successfully."