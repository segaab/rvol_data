import pandas as pd
from yahooquery import Ticker
from datetime import datetime, timezone, timedelta
from supabase._sync.client import create_client
import os
from dotenv import load_dotenv
import numpy as np
import time

ROLLING_WINDOW = 120

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in environment variables.")

# Ticker and ETF maps (from update_rvol.py)
TICKER_MAP = {
    "GOLD - COMMODITY EXCHANGE INC.": "GC=F",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "6E=F",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6A=F",
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE": "BTC-USD",
    "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE": "MBT=F",
    "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE": "ETH-USD",
    "SILVER - COMMODITY EXCHANGE INC.": "SI=F",
    "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE": "CL=F",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "6J=F",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6C=F",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE": "6B=F",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.": "DX-Y.NYB",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6N=F",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "6S=F",
    "DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE": "^DJI",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "ES=F",
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE": "NQ=F",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": "^N225",
    "SPDR S&P 500 ETF TRUST": "SPY"
}
ETF_MAP = {
    "GC=F": ("GLD", "SPDR Gold Trust"),
    "SI=F": ("SLV", "iShares Silver Trust"),
    "CL=F": ("USO", "United States Oil Fund"),
    "6E=F": ("FXE", "Invesco CurrencyShares Euro"),
    "6A=F": ("FXA", "Invesco CurrencyShares AUD"),
    "6J=F": ("FXY", "Invesco CurrencyShares JPY"),
    "6C=F": ("FXC", "Invesco CurrencyShares CAD"),
    "6B=F": ("FXB", "Invesco CurrencyShares GBP"),
    "6N=F": ("FXA", "Invesco CurrencyShares AUD"),
    "6S=F": ("FXF", "Invesco CurrencyShares CHF"),
    "DX-Y.NYB": ("UUP", "Invesco DB US Dollar Bullish"),
    "BTC-USD": ("BITO", "ProShares Bitcoin Strategy ETF"),
    "MBT=F": ("BITO", "ProShares Bitcoin Strategy ETF"),
    "ETH-USD": ("ETHE", "Grayscale Ethereum Trust"),
    "^DJI": ("IYR", "iShares U.S. Real Estate ETF"),
    "ES=F": ("SPY", "SPDR S&P 500 ETF Trust"),
    "NQ=F": ("QQQ", "Invesco QQQ Trust"),
    "^N225": ("EWJ", "iShares MSCI Japan ETF"),
    "SPY": ("SPY", "SPDR S&P 500 ETF Trust"),
}

# Connect to Supabase
def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# Helper to check for duplicates in rvol_data
def get_existing_datetimes(supabase, ticker):
    try:
        # Only fetch the last 5 days worth of datetimes for this ticker
        response = supabase.table("rvol_data").select("datetime").eq("ticker", ticker).execute()
        if hasattr(response, 'data') and response.data:
            return set(row["datetime"] for row in response.data if "datetime" in row)
        elif isinstance(response, dict) and "data" in response:
            return set(row["datetime"] for row in response["data"] if "datetime" in row)
        else:
            return set()
    except Exception as e:
        print(f"[ERROR] Could not fetch existing datetimes for {ticker}: {e}")
        return set()

# Helper to fetch, process, and insert for a single symbol
def process_symbol(symbol, name=None, days=5):
    print(f"\n[INFO] Fetching {days} days (1h interval) of data for {symbol} ({name or symbol})...")
    t = Ticker(symbol, timeout=60)
    hist = t.history(period=f"{days}d", interval="1h")
    if isinstance(hist, pd.DataFrame) and not hist.empty:
        if isinstance(hist.index, pd.MultiIndex):
            hist = hist.reset_index()
        hist = hist.rename(columns={"symbol": "ticker"})
        hist["avg_volume"] = hist["volume"].rolling(ROLLING_WINDOW).mean()
        hist["rvol"] = hist["volume"] / hist["avg_volume"]
        hist["name"] = name or symbol
        hist = hist.replace([np.inf, -np.inf], pd.NA)
        hist = hist.dropna(subset=["avg_volume", "rvol", "volume", "date"])
        hist = hist[hist["volume"] > 0]
        hist["datetime"] = pd.to_datetime(hist["date"], errors="coerce", utc=True)
        hist = hist.dropna(subset=["datetime"])
        latest_utc = hist["datetime"].max()
        print(f"[INFO] Latest available datetime in UTC: {latest_utc}")
        hist["datetime_gmt3"] = hist["datetime"] + timedelta(hours=3)
        latest_gmt3 = hist["datetime_gmt3"].max()
        print(f"[INFO] Latest available datetime in GMT+3: {latest_gmt3}")
        hist["datetime_gmt3"] = hist["datetime_gmt3"].dt.strftime("%Y-%m-%dT%H:%M:%S+03:00")
        hist["volume"] = pd.to_numeric(hist["volume"], errors="coerce").fillna(0).astype(int)
        hist["avg_volume"] = pd.to_numeric(hist["avg_volume"], errors="coerce").astype(float)
        hist["rvol"] = pd.to_numeric(hist["rvol"], errors="coerce").astype(float)
        df = hist[["ticker", "name", "datetime_gmt3", "volume", "avg_volume", "rvol"]].copy()
        df = df.rename(columns={"datetime_gmt3": "datetime"})
        print(f"[INFO] Prepared {len(df)} rows for insertion. Sample:")
        print(df.head(3))
        print(df.tail(3))
        supabase = get_supabase_client()
        existing_datetimes = get_existing_datetimes(supabase, symbol)
        # Only insert rows that are not already present
        new_records = df[~df["datetime"].isin(existing_datetimes)]
        print(f"[INFO] {len(new_records)} new rows to insert for {symbol} (skipping {len(df)-len(new_records)} duplicates)")
        records = new_records.to_dict(orient="records")
        batch_size = 200
        inserted = 0
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            try:
                result = supabase.table("rvol_data").insert(batch).execute()
                print(f"[INFO] Inserted batch {i//batch_size+1}: {len(batch)} rows")
                inserted += len(batch)
            except Exception as e:
                print(f"[ERROR] Failed to insert batch {i//batch_size+1} for {symbol}: {e}")
        print(f"[RESULT] Total new rows inserted for {symbol}: {inserted}")
    else:
        print(f"[WARN] No data found for {symbol}.")

if __name__ == "__main__":
    # Process all assets
    for name, symbol in TICKER_MAP.items():
        process_symbol(symbol, name, days=5)
        time.sleep(2)  # Avoid rate limits
    # Process all ETFs (unique symbols only)
    etf_symbols = set([v[0] for v in ETF_MAP.values()])
    for etf_symbol in etf_symbols:
        process_symbol(etf_symbol, days=5)
        time.sleep(2) 