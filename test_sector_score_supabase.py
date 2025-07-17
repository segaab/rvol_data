import pandas as pd
from yahooquery import Ticker
from datetime import datetime, timezone, timedelta

FX_ASSETS = [
    "6E=F", "6A=F", "6J=F", "6C=F", "6B=F", "6N=F", "6S=F"
]
ETF = "FXE"
DAYS = 5
ROLLING_WINDOW = 120

symbols = FX_ASSETS + [ETF]

for symbol in symbols:
    print(f"\n[INFO] Fetching last {DAYS} days of 1-hour data for {symbol}...")
    t = Ticker(symbol, timeout=60)
    hist = t.history(period=f"{DAYS}d", interval="1h")
    if isinstance(hist, pd.DataFrame) and not hist.empty:
        if isinstance(hist.index, pd.MultiIndex):
            hist = hist.reset_index()
        hist = hist.rename(columns={"symbol": "ticker"})
        hist = hist.dropna(subset=["volume", "date"])
        hist = hist[hist["volume"] > 0]
        hist["datetime"] = pd.to_datetime(hist["date"], errors="coerce", utc=True)
        hist = hist.dropna(subset=["datetime"])
        hist = hist.sort_values("datetime")
        # Convert to GMT+3
        hist["datetime_gmt3"] = hist["datetime"] + timedelta(hours=3)
        hist["datetime_gmt3"] = hist["datetime_gmt3"].dt.strftime("%Y-%m-%dT%H:%M:%S+03:00")
        # Calculate avg_volume and rvol
        hist["avg_volume"] = hist["volume"].rolling(ROLLING_WINDOW).mean()
        hist["rvol"] = hist["volume"] / hist["avg_volume"]
        print(hist[["datetime_gmt3", "volume", "avg_volume", "rvol"]].tail(10))
    else:
        print(f"[WARN] No data found for {symbol}.") 