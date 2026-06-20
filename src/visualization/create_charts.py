import os
import sys

import matplotlib.pyplot as plt
import pandas as pd


DEFAULT_EDA_OUTPUT_PATH = "outputs/eda"
DEFAULT_CHART_OUTPUT_PATH = "outputs/charts"


def ensure_output_dir(output_path: str):
    os.makedirs(output_path, exist_ok=True)


def read_csv_if_exists(file_path: str):
    if not os.path.exists(file_path):
        print(f"Skipping missing file: {file_path}")
        return None

    return pd.read_csv(file_path)


def save_trip_volume_by_hour(eda_path: str, chart_path: str):
    df = read_csv_if_exists(os.path.join(eda_path, "trip_volume_by_hour.csv"))
    if df is None or df.empty:
        return

    plt.figure()
    plt.bar(df["pickup_hour"], df["trip_count"])
    plt.xlabel("Pickup Hour")
    plt.ylabel("Trip Count")
    plt.title("Trip Volume by Pickup Hour")
    plt.tight_layout()
    plt.savefig(os.path.join(chart_path, "trip_volume_by_hour.png"))
    plt.close()


def save_top_pickup_locations(eda_path: str, chart_path: str):
    df = read_csv_if_exists(os.path.join(eda_path, "top_pickup_locations.csv"))
    if df is None or df.empty:
        return

    df = df.head(10)

    plt.figure()
    plt.bar(df["PULocationID"].astype(str), df["total_revenue"])
    plt.xlabel("Pickup Location ID")
    plt.ylabel("Total Revenue")
    plt.title("Top 10 Pickup Locations by Revenue")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(chart_path, "top_pickup_locations_by_revenue.png"))
    plt.close()


def save_distance_bucket_metrics(eda_path: str, chart_path: str):
    df = read_csv_if_exists(os.path.join(eda_path, "distance_bucket_metrics.csv"))
    if df is None or df.empty:
        return

    plt.figure()
    plt.bar(df["distance_bucket"], df["trip_count"])
    plt.xlabel("Distance Bucket")
    plt.ylabel("Trip Count")
    plt.title("Trip Count by Distance Bucket")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(chart_path, "distance_bucket_trip_count.png"))
    plt.close()


def save_tip_band_by_payment_type(eda_path: str, chart_path: str):
    df = read_csv_if_exists(os.path.join(eda_path, "tip_band_by_payment_type.csv"))
    if df is None or df.empty:
        return

    grouped = df.groupby("tip_band")["trip_count"].sum().reset_index()

    plt.figure()
    plt.bar(grouped["tip_band"], grouped["trip_count"])
    plt.xlabel("Tip Band")
    plt.ylabel("Trip Count")
    plt.title("Trip Count by Tip Band")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(os.path.join(chart_path, "tip_band_trip_count.png"))
    plt.close()


def save_day_hour_revenue(eda_path: str, chart_path: str):
    df = read_csv_if_exists(os.path.join(eda_path, "day_hour_revenue.csv"))
    if df is None or df.empty:
        return

    grouped = (
        df.groupby("pickup_hour")["total_revenue"]
        .sum()
        .reset_index()
        .sort_values("pickup_hour")
    )

    plt.figure()
    plt.plot(grouped["pickup_hour"], grouped["total_revenue"], marker="o")
    plt.xlabel("Pickup Hour")
    plt.ylabel("Total Revenue")
    plt.title("Revenue Trend by Pickup Hour")
    plt.tight_layout()
    plt.savefig(os.path.join(chart_path, "revenue_by_pickup_hour.png"))
    plt.close()


def main():
    eda_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_EDA_OUTPUT_PATH
    chart_path = sys.argv[2] if len(sys.argv) > 2 else DEFAULT_CHART_OUTPUT_PATH

    print(f"Reading EDA outputs from: {eda_path}")
    print(f"Writing charts to: {chart_path}")

    ensure_output_dir(chart_path)

    save_trip_volume_by_hour(eda_path, chart_path)
    save_top_pickup_locations(eda_path, chart_path)
    save_distance_bucket_metrics(eda_path, chart_path)
    save_tip_band_by_payment_type(eda_path, chart_path)
    save_day_hour_revenue(eda_path, chart_path)

    print("Chart generation completed.")


if __name__ == "__main__":
    main()