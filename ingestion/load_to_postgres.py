import pandas as pd
from sqlalchemy import text
from ingestion.utils import get_db_engine
from ingestion.aemet import fetch_all_stations
from ingestion.ine import fetch_hotel_occupancy

def load_dataframe(df: pd.DataFrame, table_name: str) -> None:
    """
    Loads a DataFrame into PostgreSQL.
    Replaces the table if it already exists.
    """
    engine = get_db_engine()
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="replace",  # Overwrites on each run — fine for raw tables
        index=False
    )
    print(f"Loaded {len(df)} rows into table '{table_name}'")

def load_aemet(year: int, stations: list) -> None:
    """Extracts AEMET data and loads it into PostgreSQL."""
    print("\n--- AEMET ---")
    df = fetch_all_stations(year, stations)
    if not df.empty:
        load_dataframe(df, "raw_aemet_climate")
    else:
        print("No AEMET data to load.")

def load_ine() -> None:
    """Extracts INE data and loads it into PostgreSQL."""
    print("\n--- INE ---")
    df = fetch_hotel_occupancy()
    if not df.empty:
        load_dataframe(df, "raw_ine_tourism")
    else:
        print("No INE data to load.")

if __name__ == "__main__":
    # Test with 3 stations for now
    # Will be replaced by full province list in main_ingest.py
    stations = ["3195", "8414A", "2867"]
    
    load_aemet(2023, stations)
    load_ine()
    
    print("\nDone. Check your database:")
    print("  Table: raw_aemet_climate")
    print("  Table: raw_ine_tourism")