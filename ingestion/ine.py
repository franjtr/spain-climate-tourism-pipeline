import requests
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

BASE_URL = "https://servicios.ine.es/wstempus/js/es"
DATA_DIR = Path("data/ine")
MAPPING_FILE = Path("dbt/seeds/province_mapping.csv") 

def _ine_request(table_id: str) -> list:
    """
    Fetches data from INE API.
    Returns raw JSON as a list of records.
    """
    url = f"{BASE_URL}/DATOS_TABLA/{table_id}?tip=AM"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def fetch_hotel_occupancy() -> pd.DataFrame:
    print("Downloading INE hotel occupancy data...")

    # Load province names from CSV
    mapping_df = pd.read_csv(MAPPING_FILE)
    PROVINCIAS_INE = set(mapping_df["province_name"].tolist())

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_file = DATA_DIR / "hotel_occupancy_raw.json"

    # Skip if already downloaded
    if raw_file.exists():
        print("Skipping INE download — file already exists")
        with open(raw_file, encoding="utf-8") as f:
            raw_data = json.load(f)
    else:
        raw_data = _ine_request("2074")
        with open(raw_file, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)

    rows = []
    for serie in raw_data:
        name = serie.get("Nombre", "")

        # Only keep series from our province list
        provincia = next((p for p in PROVINCIAS_INE if name.startswith(p + ".")), None)
        if not provincia:
            continue

        # Only Viajeros.Total and Pernoctaciones.Total
        if "Viajeros. Total." not in name and "Pernoctaciones. Total." not in name:
            continue

        for dato in serie.get("Data", []):
            rows.append({
                "serie_name": name,
                "provincia": provincia,
                "metric": "viajeros" if "Viajeros" in name else "pernoctaciones",
                "year": dato.get("Anyo"),
                "month": dato.get("T3_Periodo"),
                "date": dato.get("Fecha"),
                "value": dato.get("Valor"),
                "secret": dato.get("Secreto")
            })

    df = pd.DataFrame(rows)
    df["extracted_at"] = datetime.now()

    print(f"Total rows downloaded: {len(df)}")
    return df

if __name__ == "__main__":
    df = fetch_hotel_occupancy()
    print(df.head(10))
    print(f"\nUnique series: {df['serie_name'].nunique()}")
    print(f"\nSample series names:")
    print(df['serie_name'].unique()[:5])