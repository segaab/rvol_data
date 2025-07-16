import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json

# Mock asset and sector definitions
ASSETS = [
    ("GC=F", "GOLD - COMMODITY EXCHANGE INC."),
    ("CL=F", "WTI FINANCIAL CRUDE OIL"),
    ("BTC-USD", "BITCOIN"),
]
SECTORS = ["Metals", "Energy", "Crypto"]
ASSET_TO_SECTOR = {"GC=F": "Metals", "CL=F": "Energy", "BTC-USD": "Crypto"}

# Generate 2 years of hourly timestamps
end = datetime.now().replace(minute=0, second=0, microsecond=0)
start = end - timedelta(days=730)
dates = pd.date_range(start, end, freq="H")

# Generate mock RVol data
rvol_rows = []
for ticker, name in ASSETS:
    for dt in dates:
        volume = random.randint(100, 10000)
        avg_volume = volume * random.uniform(0.8, 1.2)
        rvol = volume / avg_volume * random.uniform(0.7, 1.3)
        rvol_rows.append({
            "ticker": ticker,
            "name": name,
            "date": dt.strftime("%d %b %Y, %H:%M"),
            "volume": volume,
            "avg_volume": avg_volume,
            "rvol": rvol
        })
rvol_df = pd.DataFrame(rvol_rows)

# Generate mock sector score data
sector_score_rows = []
for sector in SECTORS:
    for dt in dates:
        etf_rvol = random.uniform(0.7, 1.3)
        mean_asset_rvol = random.uniform(0.7, 1.3)
        sector_score = 0.4 * etf_rvol + 0.6 * mean_asset_rvol
        sector_score_rows.append({
            "sector": sector,
            "date": dt.strftime("%Y-%m-%d"),
            "hour": dt.hour,
            "etf_rvol": etf_rvol,
            "mean_asset_rvol": mean_asset_rvol,
            "sector_score": sector_score
        })
sector_score_df = pd.DataFrame(sector_score_rows)

# Save as CSV for easy loading
rvol_df.to_csv("mock_rvol_data.csv", index=False)
sector_score_df.to_csv("mock_sector_score_data.csv", index=False)

print("Mock data generated: mock_rvol_data.csv, mock_sector_score_data.csv") 