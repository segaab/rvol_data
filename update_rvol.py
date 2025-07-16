import pandas as pd
from yahooquery import Ticker
import yahooquery
from supabase._sync.client import create_client
import os
from datetime import datetime
from dotenv import load_dotenv
from postgrest.exceptions import APIError
import json

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
def fetch_and_process_ticker(name, symbol):
    print(f"Fetching data for {name} ({symbol})...")
    t = Ticker(symbol, timeout=60)
    hist = t.history(period="730d", interval="1h")
    if hist.empty:
        print(f"No data for {symbol}")
        return None
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index()
    hist = hist.rename(columns={"symbol": "ticker"})
    hist["avg_volume"] = hist["volume"].rolling(ROLLING_WINDOW).mean()
    hist["rvol"] = hist["volume"] / hist["avg_volume"]
    hist["name"] = name
    # Format date as 'DD Mon YYYY, HH:MM'
    def format_friendly_date(x):
        if hasattr(x, "strftime"):
            return x.strftime("%d %b %Y, %H:%M")
        try:
            dt = pd.to_datetime(x)
            return dt.strftime("%d %b %Y, %H:%M")
        except Exception:
            return str(x)
    hist["date"] = hist["date"].apply(format_friendly_date)
    hist = hist.replace([float('inf'), float('-inf')], pd.NA)
    hist = hist.dropna(subset=["avg_volume", "rvol", "volume", "date"])
    # If latest entry has no volume, drop just that row and warn
    if not hist.empty and (pd.isna(hist.iloc[-1]["volume"]) or hist.iloc[-1]["volume"] == 0):
        print(f"Warning: Skipping latest entry for {name} ({symbol}) due to missing volume.")
        hist = hist.iloc[:-1]
    if hist.empty:
        print(f"No valid data to insert for {name} ({symbol}).")
        return None
    return hist[["ticker", "name", "date", "volume", "avg_volume", "rvol"]]

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
                    print(f"Duplicate found for {record['ticker']} {record['date']}, skipping.")
                elif "invalid input syntax for type bigint" in str(e2):
                    print(f"Invalid volume for {record['ticker']} {record['date']}, skipping.")
                else:
                    raise

def load_json(filename):
    with open(filename, "r") as f:
        return json.load(f)

def calculate_and_insert_sector_scores(df, supabase):
    asset_category_map = load_json("asset_category_map.json")
    asset_etf_map = load_json("asset_etf_map.json")
    for sector, assets in asset_category_map.items():
        # Use the first asset's ETF as the sector ETF
        etf_symbol = asset_etf_map[assets[0]][0] if assets[0] in asset_etf_map else None
        sector_df = df[df["ticker"].isin(assets)]
        if sector_df.empty or not etf_symbol:
            continue
        latest_day = sector_df["date"].dt.date.max()
        day_df = sector_df[sector_df["date"].dt.date == latest_day].copy()
        if day_df.empty:
            continue
        for hour in sorted(day_df["date"].dt.hour.unique()):
            hour_df = day_df[day_df["date"].dt.hour == hour]
            mean_asset_rvol = hour_df["rvol"].mean()
            etf_rvol = None
            etf_df = df[(df["ticker"] == etf_symbol) & (df["date"].dt.date == latest_day) & (df["date"].dt.hour == hour)]
            if not etf_df.empty:
                etf_rvol = etf_df["rvol"].mean()
            if etf_rvol is not None and not pd.isna(mean_asset_rvol):
                sector_score = 0.4 * etf_rvol + 0.6 * mean_asset_rvol
                record = {
                    "sector": sector,
                    "date": str(latest_day),
                    "hour": int(hour),
                    "etf_rvol": float(etf_rvol),
                    "mean_asset_rvol": float(mean_asset_rvol),
                    "sector_score": float(sector_score)
                }
                supabase.table("sector_score_data").insert(record).execute()
                print(f"Inserted sector score for {sector} {latest_day} hour {hour}: {sector_score}")

# --- Main script ---
def main():
    supabase = get_supabase_client()
    # Delete all rows from rvol_data and sector_score_data before loading new data
    try:
        supabase.table("rvol_data").delete().neq("ticker", "").execute()
        print("All rows deleted from rvol_data.")
    except Exception as e:
        print(f"Error deleting all rows from rvol_data: {e}")
    try:
        supabase.table("sector_score_data").delete().neq("sector", "").execute()
        print("All rows deleted from sector_score_data.")
    except Exception as e:
        print(f"Error deleting all rows from sector_score_data: {e}")
    # Fetch and insert for assets
    for name, symbol in TICKER_MAP.items():
        try:
            df = fetch_and_process_ticker(name, symbol)
            if df is not None:
                print(f"Fetched {len(df)} rows for {name} ({symbol})")
                upsert_multiple_rows(df, supabase)
        except Exception as e:
            print(f"Error processing {name} ({symbol}): {e}")
    # Fetch and insert for ETFs
    for fut_symbol, (etf_symbol, etf_name) in ETF_MAP.items():
        try:
            df = fetch_and_process_ticker(etf_name, etf_symbol)
            if df is not None:
                print(f"Fetched {len(df)} rows for {etf_name} ({etf_symbol})")
                upsert_multiple_rows(df, supabase)
        except Exception as e:
            print(f"Error processing ETF {etf_name} ({etf_symbol}): {e}")
    # Calculate and insert sector scores
    response = supabase.table("rvol_data").select("ticker, date, rvol").execute()
    df = pd.DataFrame(response.data)
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.dropna(subset=["date", "rvol"])
        df["rvol"] = df["rvol"].astype(float)
        calculate_and_insert_sector_scores(df, supabase)

if __name__ == "__main__":
    main() 