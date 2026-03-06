import pandas as pd
from pathlib import Path
from ingestion.load_to_postgres import load_aemet, load_ine
from datetime import datetime

START_YEAR = 2019
END_YEAR = datetime.now().year - 1
YEARS = list(range(START_YEAR, END_YEAR + 1))

MAPPING_FILE = Path("dbt/seeds/province_mapping.csv")

def run_pipeline():
    """
    Main entry point for the ingestion pipeline.
    Reads station list from province_mapping.csv and loads all data into PostgreSQL.
    """
    print("Starting ingestion pipeline...")

    # Load station IDs from province mapping
    mapping_df = pd.read_csv(MAPPING_FILE)
    stations = mapping_df["station_id"].astype(str).tolist()
    print(f"Found {len(stations)} stations to download")

    # Run ingestion
    for year in YEARS:
        load_aemet(year, stations)
    load_ine()

    print("\nPipeline completed successfully.")
    print(f"  AEMET: {len(stations)} stations x {len(YEARS)} years ({YEARS[0]}-{YEARS[-1]})")
    print(f"  INE: hotel occupancy data loaded")

if __name__ == "__main__":
    run_pipeline()