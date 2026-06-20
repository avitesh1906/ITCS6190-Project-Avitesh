INPUT_PATH ?= data/raw/yellow_tripdata_2024-01.parquet
EDA_OUTPUT_PATH ?= outputs/eda
ML_OUTPUT_PATH ?= outputs/ml
CHART_OUTPUT_PATH ?= outputs/charts

.PHONY: install ingest eda stream ml charts run clean

install:
	pip install -r requirements.txt

ingest:
	python src/ingestion/load_taxi_data.py $(INPUT_PATH)

eda:
	python src/eda/basic_eda.py $(INPUT_PATH) $(EDA_OUTPUT_PATH)

stream:
	python src/streaming/file_streaming_simulation.py 5

ml:
	python src/ml_pipeline.py $(INPUT_PATH) $(ML_OUTPUT_PATH)

charts:
	python src/visualization/create_charts.py $(EDA_OUTPUT_PATH) $(CHART_OUTPUT_PATH)

run: ingest eda ml charts
	@echo "Pipeline completed successfully."

clean:
	powershell -Command "Remove-Item -Recurse -Force outputs/eda, outputs/ml, outputs/charts, outputs/checkpoints -ErrorAction SilentlyContinue"