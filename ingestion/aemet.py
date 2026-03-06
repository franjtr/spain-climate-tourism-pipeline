import requests
import json
import pandas as pd
import time
from pathlib import Path
from datetime import datetime
from ingestion.utils import get_aemet_key

BASE_URL = "https://opendata.aemet.es/opendata/api"
DATA_DIR = Path("data/aemet")

def _aemet_request(endpoint: str) -> dict:

    # Deals with the double call protocol in Aemet

    api_key = get_aemet_key()
    headers = {"api_key": api_key}
    
    # First call
    response = requests.get(f"{BASE_URL}{endpoint}", headers=headers)
    response.raise_for_status()
    
    data_url = response.json().get("datos")
    if not data_url:
        raise ValueError(f"AEMET did not return URL for: {endpoint}")
    
    # Second call (real data)
    data_response = requests.get(data_url)
    data_response.raise_for_status()
    
    return data_response.json()

def fetch_monthly_climate(year: int, station_id: str) -> pd.DataFrame:
    """
    Downloads climate date for a station and year, the JSON file is saved
    in data/aemet
    """
    print(f"Downloading station {station_id} for {year}...")
    
    endpoint = f"/valores/climatologicos/mensualesanuales/datos/anioini/{year}/aniofin/{year}/estacion/{station_id}"
    raw_data = _aemet_request(endpoint)
    
    # Guardar JSON crudo en data/ (landing zone)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / f"climate_{station_id}_{year}.json"
    with open(raw_file, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False, indent=2)
    
    # Convertir a DataFrame
    df = pd.DataFrame(raw_data)
    df["extracted_at"] = datetime.now()
    
    return df

def fetch_all_stations(year: int, stations: list) -> pd.DataFrame:
    """
    Loops a list of stations and combines the results
    """
    all_frames = []
    
    for station_id in stations:
        try:
            df = fetch_monthly_climate(year, station_id)
            all_frames.append(df)
        except Exception as e:
            print(f"Error downloading station {station_id}: {e}")
        finally:
            time.sleep(1.5)  
    
    return pd.concat(all_frames, ignore_index=True)

if __name__ == "__main__":
    stations = ["3195", "8414A", "2867"]  
    df = fetch_all_stations(2023, stations)
    print(df.head())
    print(f"Total rows downloaded: {len(df)}")