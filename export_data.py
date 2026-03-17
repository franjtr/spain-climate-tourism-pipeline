import pandas as pd
import sys
sys.path.append('.')
from ingestion.utils import get_db_engine

engine = get_db_engine()
df = pd.read_sql("SELECT * FROM mart_weather_tourism", engine)
df.to_parquet("dashboard/mart_weather_tourism.parquet", index=False)
print(f"Exported: {len(df)} rows to dashboard/mart_weather_tourism.parquet")