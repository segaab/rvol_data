import pandas as pd
from yahooquery import Ticker
import yahooquery
from supabase._sync.client import create_client
import os
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
from postgrest.exceptions import APIError
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import numpy as np

# Load environment variables from .env file
load_dotenv()

# Debug print to check environment variables
print("SUPABASE_URL:", os.getenv("SUPABASE_URL"))
print("SUPABASE_SERVICE_ROLE_KEY:", (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "")[:6], "...")

# --- Supabase credentials (replace with your values or use environment variables) ---
SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"

# --- Rolling window for average volume ---
ROLLING_WINDOW = 120  # 5 days * 24 hours

# --- Ticker mapping: Name -> Yahoo Finance Symbol ---
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
    "6N=F": ("FXA", "Invesco CurrencyShares AUD"),  # or DBC as a proxy
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

# --- Connect to Supabase ---
def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# --- Fetch and process 2 years of 1-hour data for a single ticker ---
def fetch_and_process_ticker(name, symbol, max_retries=3):
    print(f"Fetching data for {name} ({symbol})...")
    
    for attempt in range(max_retries):
        try:
    t = Ticker(symbol, timeout=60)
    hist = t.history(period="730d", interval="1h")  # 2 years of 1-hour data
    if hist.empty:
        print(f"No data for {symbol}")
        return None
            break  # Success, exit retry loop
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff: 5s, 10s, 15s
                print(f"Attempt {attempt + 1} failed for {symbol}: {e}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                print(f"All {max_retries} attempts failed for {symbol}: {e}")
                return None
    
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index()
    hist = hist.rename(columns={"symbol": "ticker"})
    hist["avg_volume"] = hist["volume"].rolling(ROLLING_WINDOW).mean()
    hist["rvol"] = hist["volume"] / hist["avg_volume"]
    hist["name"] = name
    # Do NOT format date yet!
    hist = hist.replace([float('inf'), float('-inf')], pd.NA)
    hist = hist.dropna(subset=["avg_volume", "rvol", "volume", "date"])
    hist = hist[hist["volume"] > 0]
    # Sort by datetime and keep all valid rows (latest 2 years)
    if isinstance(hist, pd.DataFrame) and "date" in hist.columns:
        hist = hist.sort_values(by="date")
    if hist.empty:
        print(f"No valid data to insert for {name} ({symbol}).")
        return None
    # Format datetime as UTC ISO8601 string
    hist["datetime"] = pd.to_datetime(hist["date"], errors="coerce", utc=True)
    hist["datetime"] = hist["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return hist[["ticker", "name", "datetime", "volume", "avg_volume", "rvol"]]

# --- Upsert multiple rows into Supabase ---
def upsert_multiple_rows(df, supabase):
    records = df.to_dict(orient="records")
    for record in records:
        record["volume"] = int(record["volume"]) if not pd.isna(record["volume"]) else None
    try:
        result = supabase.table("rvol_data").insert(records).execute()
        print("Insert result:", result)
    except APIError as e:
        print("Error inserting records:", e)
        for record in records:
            try:
                supabase.table("rvol_data").insert(record).execute()
            except APIError as e2:
                if "duplicate key value violates unique constraint" in str(e2):
                    print(f"Duplicate found for {record['ticker']} {record['datetime']}, skipping.")
                elif "invalid input syntax for type bigint" in str(e2):
                    print(f"Invalid volume for {record['ticker']} {record['datetime']}, skipping.")
                else:
                    raise

def upsert_multiple_rows_batched(df, supabase, batch_size=200):
    # Ensure volume is int and not float string
    if isinstance(df, pd.DataFrame) and "volume" in df.columns:
        vol = pd.Series(df["volume"])
        vol = pd.to_numeric(vol, errors="coerce")
        if isinstance(vol, pd.Series):
            vol = vol.fillna(0).astype(int)
        df["volume"] = vol
    records = df.to_dict(orient="records")
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            result = supabase.table("rvol_data").insert(batch).execute()
            print(f"Inserted batch {i//batch_size+1}: {len(batch)} rows")
        except APIError as e:
            print("Error inserting batch:", e)
        time.sleep(1)  # Add a short delay to avoid server disconnects

# Threaded fetch/process for one asset
def fetch_and_insert_asset(args):
    name, symbol, supabase = args
    try:
        df = fetch_and_process_ticker(name, symbol)
        if df is not None:
            print(f"Fetched {len(df)} rows for {name} ({symbol})")
            upsert_multiple_rows_batched(df, supabase)
        return name, symbol, len(df) if df is not None else 0
    except Exception as e:
        print(f"Error processing {name} ({symbol}): {e}")
        return name, symbol, 0

def load_json(filename):
    with open(filename, "r") as f:
        return json.load(f)

# --- Main script ---
def main():
    supabase = get_supabase_client()
    tickers = list(TICKER_MAP.items())
    # Parallel fetch/process, but batch insert per asset
    with ThreadPoolExecutor(max_workers=1) as executor:  # Reduced to 1 worker to avoid API rate limits
        futures = [executor.submit(fetch_and_insert_asset, (name, symbol, supabase)) for name, symbol in tickers]
        for future in as_completed(futures):
            name, symbol, nrows = future.result()
            print(f"Done: {name} ({symbol}) - {nrows} rows processed.")
            time.sleep(2)  # Add delay between assets to avoid rate limiting
    # Fetch and insert for ETFs (sequentially, or you can parallelize similarly)
    for fut_symbol, (etf_symbol, etf_name) in ETF_MAP.items():
        try:
            df = fetch_and_process_ticker(etf_name, etf_symbol)
            if df is not None:
                print(f"Fetched {len(df)} rows for {etf_name} ({etf_symbol})")
                if isinstance(df, pd.DataFrame) and "volume" in df.columns:
                    vol = pd.Series(df["volume"])
                    vol = pd.to_numeric(vol, errors="coerce")
                    if isinstance(vol, pd.Series):
                        vol = vol.fillna(0).astype(int)
                    df["volume"] = vol
                upsert_multiple_rows_batched(df, supabase)
        except Exception as e:
            print(f"Error processing ETF {etf_name} ({etf_symbol}): {e}")
            continue  # Continue with next ETF even if one fails
        time.sleep(2)  # Add delay between ETF fetches to avoid rate limiting
    # Sector score calculation and insertion removed

if __name__ == "__main__":
    main() 