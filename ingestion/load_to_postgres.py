import pandas as pd
from sqlalchemy import text
from ingestion.utils import get_db_engine
from ingestion.aemet import fetch_all_stations
from ingestion.ine import fetch_hotel_occupancy
from sqlalchemy import text

def load_dataframe(df: pd.DataFrame, table_name: str, first_load: bool = False) -> None:
    engine = get_db_engine()
    
    with engine.begin() as conn:
        result = conn.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{table_name}')"))
        exists = result.scalar()
        
        if exists and first_load:
            conn.execute(text(f"TRUNCATE {table_name}"))
    
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False
    )
    print(f"Loaded {len(df)} rows into table '{table_name}'")

def load_aemet(year: int, stations: list, first_load: bool = False) -> None:
    print(f"\n--- AEMET {year} ---")
    df = fetch_all_stations(year, stations)
    if not df.empty:
        load_dataframe(df, "raw_aemet_climate", first_load=first_load)

def load_ine() -> None:
    """Extracts INE data and loads it into PostgreSQL."""
    print("\n--- INE ---")
    df = fetch_hotel_occupancy()
    if not df.empty:
        load_dataframe(df, "raw_ine_tourism")
    else:
        print("No INE data to load.")

