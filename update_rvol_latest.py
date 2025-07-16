import pandas as pd
from yahooquery import Ticker
import yahooquery
from supabase._sync.client import create_client
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from postgrest.exceptions import APIError

# Force yahooquery to use requests backend instead of curl_cffi
yahooquery.Ticker._USE_CURL_CFFI = False

# Load environment variables from .env file
load_dotenv()

SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
ROLLING_WINDOW = 120  # 5 days * 24 hours

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
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": "^N225"
}

def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def format_friendly_date(x):
    if hasattr(x, "strftime"):
        return x.strftime("%d %b %Y, %H:%M")
    try:
        dt = pd.to_datetime(x)
        return dt.strftime("%d %b %Y, %H:%M")
    except Exception:
        return str(x)

def fetch_latest_with_volume(name, symbol):
    print(f"Fetching latest 1H data for {name} ({symbol})...")
    t = Ticker(symbol, timeout=60)
    hist = t.history(period="7d", interval="1h")
    if hist.empty:
        print(f"No data for {symbol}")
        return None
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index()
    hist = hist.rename(columns={"symbol": "ticker"})
    hist["avg_volume"] = hist["volume"].rolling(ROLLING_WINDOW).mean()
    hist["rvol"] = hist["volume"] / hist["avg_volume"]
    hist["name"] = name
    hist["date"] = hist["date"].apply(format_friendly_date)
    hist = hist.replace([float('inf'), float('-inf')], pd.NA)
    hist = hist.dropna(subset=["avg_volume", "rvol", "volume", "date"])
    # Get the latest row with volume > 0
    hist_nonzero = hist[hist["volume"] > 0]
    if hist_nonzero.empty:
        print(f"No recent nonzero volume data for {name} ({symbol})")
        return None
    latest = hist_nonzero.iloc[[-1]][["ticker", "name", "date", "volume", "avg_volume", "rvol"]]
    return latest

def upsert_single_row(df, supabase):
    record = df.to_dict(orient="records")[0]
    record["volume"] = int(record["volume"]) if not pd.isna(record["volume"]) else None
    try:
        result = supabase.table("rvol_data").insert(record).execute()
        print("Insert result:", result)
    except APIError as e:
        if "duplicate key value violates unique constraint" in str(e):
            print(f"Duplicate found for {record['ticker']} {record['date']}, skipping.")
        elif "invalid input syntax for type bigint" in str(e):
            print(f"Invalid volume for {record['ticker']} {record['date']}, skipping.")
        else:
            raise

# FIFO deletion: keep only entries from the last 2 years for each asset (optional, can be removed if not needed)
def delete_older_than_2_years(supabase, symbol):
    cutoff = datetime.utcnow() - timedelta(days=730)
    cutoff_str = cutoff.strftime("%d %b %Y, %H:%M")
    try:
        result = supabase.table("rvol_data").delete().match({"ticker": symbol}).lt("date", cutoff_str).execute()
        print(f"Deleted entries for {symbol} older than 2 years (if any).")
    except Exception as e:
        print(f"Error deleting old entries for {symbol}: {e}")

# Main: only fetch and insert the latest entry with volume for each asset
def main():
    supabase = get_supabase_client()
    for name, symbol in TICKER_MAP.items():
        try:
            df = fetch_latest_with_volume(name, symbol)
            if df is not None:
                print("Fetched row:")
                print(df)
                upsert_single_row(df, supabase)
            delete_older_than_2_years(supabase, symbol)
        except Exception as e:
            print(f"Error processing {name} ({symbol}): {e}")

if __name__ == "__main__":
    main() 