param(
    [string]$InputPath = "data/raw/yellow_tripdata_2024-01.parquet",
    [string]$EdaOutputPath = "outputs/eda",
    [string]$MlOutputPath = "outputs/ml",
    [string]$ChartOutputPath = "outputs/charts"
)

Write-Host "========================================"
Write-Host "NYC Taxi Spark Pipeline"
Write-Host "========================================"
Write-Host "Input path: $InputPath"
Write-Host "EDA output path: $EdaOutputPath"
Write-Host "ML output path: $MlOutputPath"
Write-Host "Chart output path: $ChartOutputPath"
Write-Host "========================================"

if (-not (Test-Path $InputPath)) {
    Write-Error "Input file not found: $InputPath"
    Write-Host "Download a Yellow Taxi Parquet file and place it under data/raw/"
    exit 1
}

Write-Host ""
Write-Host "Step 1: Validate ingestion"
python src/ingestion/load_taxi_data.py $InputPath

if ($LASTEXITCODE -ne 0) {
    Write-Error "Ingestion step failed."
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Step 2: Run EDA"
python src/eda/basic_eda.py $InputPath $EdaOutputPath

if ($LASTEXITCODE -ne 0) {
    Write-Error "EDA step failed."
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Step 3: Run MLlib fare prediction"
python src/ml_pipeline.py $InputPath $MlOutputPath

if ($LASTEXITCODE -ne 0) {
    Write-Error "MLlib step failed."
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Step 4: Generate visualizations"
python src/visualization/create_charts.py $EdaOutputPath $ChartOutputPath

if ($LASTEXITCODE -ne 0) {
    Write-Error "Visualization step failed."
    exit $LASTEXITCODE
}

Write-Host ""
Write-Host "Pipeline completed successfully."