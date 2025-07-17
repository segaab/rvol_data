 
import pandas as pd
from yahooquery import Ticker
from supabase._sync.client import create_client
import os
from datetime import datetime, time, timezone
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time as pytime
import numpy as np

# Load environment variables
load_dotenv()

# Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL") or "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"

# Rolling window for average volume calculation
ROLLING_WINDOW = 120

def load_json(filename):
    with open(filename, "r") as f:
        return json.load(f)

def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_and_process_ticker(name, symbol, max_retries=3):
    print(f"[FETCH] Fetching data for {name} ({symbol})...")
    
    for attempt in range(max_retries):
        try:
            t = Ticker(symbol, timeout=60)
            hist = t.history(period="730d", interval="1h")  # 2 years of 1-hour data
            if hist.empty:
                print(f"[FETCH] No data for {symbol}")
                return None
            break  # Success, exit retry loop
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 5  # Exponential backoff: 5s, 10s, 15s
                print(f"[FETCH] Attempt {attempt + 1} failed for {symbol}: {e}. Retrying in {wait_time}s...")
                pytime.sleep(wait_time)
            else:
                print(f"[FETCH] All {max_retries} attempts failed for {symbol}: {e}")
                return None
    
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index()
    hist = hist.rename(columns={"symbol": "ticker"})
    hist["avg_volume"] = hist["volume"].rolling(ROLLING_WINDOW).mean()
    hist["rvol"] = hist["volume"] / hist["avg_volume"]
    hist["name"] = name  # Use symbol as name for sector score script
    # Do NOT format date yet!
    hist = hist.replace([np.inf, -np.inf], pd.NA)
    hist = hist.dropna(subset=["avg_volume", "rvol", "volume", "date"])
    hist = hist[hist["volume"] > 0]
    # Sort by datetime and keep all valid rows (latest 2 years)
    if isinstance(hist, pd.DataFrame) and "date" in hist.columns:
        hist = hist.sort_values(by="date")
    if hist.empty:
        print(f"[FETCH] No valid data to insert for {name} ({symbol}).")
        return None
    # Add 'datetime' column as UTC ISO8601 string
    hist["datetime"] = pd.to_datetime(hist["date"], errors="coerce", utc=True)
    hist["datetime"] = hist["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    return hist[["ticker", "name", "datetime", "volume", "avg_volume", "rvol"]]

def upsert_sector_scores_batched(records, supabase, batch_size=500):
    """Upsert sector scores in batches"""
    for i in range(0, len(records), batch_size):
        batch = records[i:i+batch_size]
        try:
            result = supabase.table("sector_score_data").upsert(batch).execute()
            print(f"[UPSERT] Upserted sector score batch {i//batch_size+1}: {len(batch)} rows")
        except Exception as e:
            print("[UPSERT] Error upserting sector score batch:", e)

def calculate_sector_scores_for_sector(args):
    """Calculate sector scores for each asset in a sector using its ETF and mean sector rvol, only for aligned datetimes."""
    sector, assets, asset_etf_map, all_data, supabase = args
    print(f"[SECTOR] Processing sector: {sector}, assets: {assets}")
    # Fetch all asset data for the sector
    sector_data = []
    for asset in assets:
        if asset in all_data:
            sector_data.append(all_data[asset])
        else:
            print(f"[SECTOR] No data for asset {asset} in sector {sector}")
    if not sector_data:
        print(f"[SECTOR] No data for sector {sector} assets")
        return sector, 0
    # Combine all sector asset data
    sector_df = pd.concat(sector_data, ignore_index=True)
    sector_df["datetime"] = pd.to_datetime(sector_df["datetime"], utc=True)
    records = []
    for asset in assets:
        etf_list = asset_etf_map.get(asset, [])
        if not etf_list:
            print(f"[SECTOR] No ETF mapping for asset {asset} in sector {sector}")
            continue
        etf = etf_list[0]
        if etf not in all_data:
            print(f"[SECTOR] No data for ETF {etf} for asset {asset}")
            continue
        asset_df = all_data[asset].copy()
        etf_df = all_data[etf].copy()
        # Patch: Check for 'datetime' column in all DataFrames
        missing_datetime = False
        for symbol, df in [(asset, asset_df), (etf, etf_df)]:
            if 'datetime' not in df.columns:
                print(f"[ERROR] {symbol} is missing 'datetime' column. Columns: {df.columns.tolist()}")
                missing_datetime = True
        for a in assets:
            if a not in all_data:
                print(f"[ERROR] {a} missing from all_data.")
                missing_datetime = True
                continue
            if 'datetime' not in all_data[a].columns:
                print(f"[ERROR] {a} is missing 'datetime' column. Columns: {all_data[a].columns.tolist()}")
                missing_datetime = True
        if missing_datetime:
            print(f"[SECTOR] Skipping asset {asset} in sector {sector} due to missing 'datetime' column(s).")
            continue
        asset_df["datetime"] = pd.to_datetime(asset_df["datetime"], utc=True)
        etf_df["datetime"] = pd.to_datetime(etf_df["datetime"], utc=True)
        # Find intersection of datetimes for all sector assets and this ETF
        datetime_sets = []
        for a in assets:
            if a in all_data and 'datetime' in all_data[a].columns:
                datetime_sets.append(set(pd.to_datetime(all_data[a]["datetime"], utc=True)))
        datetime_sets.append(set(etf_df["datetime"]))
        if not datetime_sets:
            continue
        common_datetimes = set.intersection(*datetime_sets)
        if not common_datetimes:
            print(f"[SECTOR] No common datetimes for asset {asset} and ETF {etf} in sector {sector}")
            continue
        for dt in sorted(common_datetimes):
            # For each asset, get rvol at dt
            rvols = []
            for a in assets:
                a_df = all_data[a]
                a_row = a_df[a_df["datetime"] == dt]
                if a_row.empty:
                    break
                rvols.append(a_row["rvol"].values[0])
            if len(rvols) != len(assets):
                continue
            mean_asset_rvol = float(np.mean(rvols))
            # Get ETF rvol at dt
            etf_row = etf_df[etf_df["datetime"] == dt]
            if etf_row.empty:
                continue
            etf_rvol = float(etf_row["rvol"].values[0])
            # Calculate sector score
            if pd.isna(mean_asset_rvol) or pd.isna(etf_rvol):
                continue
            sector_score = 0.4 * etf_rvol + 0.6 * mean_asset_rvol
            record = {
                "sector": sector,
                "asset": asset,
                "datetime": dt.isoformat(),
                "etf": etf,
                "mean_asset_rvol": mean_asset_rvol,
                "etf_rvol": etf_rvol,
                "sector_score": sector_score
            }
            records.append(record)
    if records:
        print(f"[SECTOR] Upserting {len(records)} sector score records for sector {sector}")
        upsert_sector_scores_batched(records, supabase)
    else:
        print(f"[SECTOR] No sector score records to upsert for {sector}")
    return sector, len(records)

# Threaded fetch/process for one asset
def fetch_and_insert_asset(args):
    symbol, supabase = args
    try:
        df = fetch_and_process_ticker(symbol, symbol)  # Use symbol as both name and symbol
        if df is not None:
            print(f"Fetched {len(df)} rows for {symbol}")
            # Ensure volume is int and not float string (same as update_rvol.py)
            if isinstance(df, pd.DataFrame) and "volume" in df.columns:
                vol = pd.Series(df["volume"])
                vol = pd.to_numeric(vol, errors="coerce")
                if isinstance(vol, pd.Series):
                    vol = vol.fillna(0).astype(int)
                df["volume"] = vol
        return symbol, len(df) if df is not None else 0
    except Exception as e:
        print(f"Error processing {symbol}: {e}")
        return symbol, 0

def process_sector_scores_sector_by_sector():
    supabase = get_supabase_client()
    asset_category_map = load_json("asset_category_map.json")
    asset_etf_map = load_json("asset_etf_map.json")
    for sector, assets in asset_category_map.items():
        print(f"\n[SECTOR] === Processing sector: {sector} ===")
        # 1. Collect all unique assets for this sector
        asset_symbols = set(assets)
        # 2. Collect all unique ETFs needed for this sector
        etf_symbols = set()
        for asset in assets:
            etf_list = asset_etf_map.get(asset, [])
            if etf_list:
                etf_symbols.add(etf_list[0])
        print(f"[SECTOR] Fetching {len(asset_symbols)} asset(s) and {len(etf_symbols)} ETF(s) for sector {sector}")
        # 3. Fetch all asset data
        asset_data = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fetch_and_process_ticker, symbol, symbol): symbol for symbol in asset_symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    df = future.result()
                    if df is not None:
                        asset_data[symbol] = df
                        print(f"[SECTOR] ✅ Fetched {len(df)} rows for asset {symbol}")
                    else:
                        print(f"[SECTOR] ❌ No data for asset {symbol}")
                except Exception as e:
                    print(f"[SECTOR] ❌ Error fetching asset {symbol}: {e}")
        # 4. Fetch all ETF data
        etf_data = {}
        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {executor.submit(fetch_and_process_ticker, symbol, symbol): symbol for symbol in etf_symbols}
            for future in as_completed(futures):
                symbol = futures[future]
                try:
                    df = future.result()
                    if df is not None:
                        etf_data[symbol] = df
                        print(f"[SECTOR] ✅ Fetched {len(df)} rows for ETF {symbol}")
                    else:
                        print(f"[SECTOR] ❌ No data for ETF {symbol}")
                except Exception as e:
                    print(f"[SECTOR] ❌ Error fetching ETF {symbol}: {e}")
        # 5. Merge asset and ETF data for processing
        all_data = {**asset_data, **etf_data}
        args = (sector, assets, asset_etf_map, all_data, supabase)
        try:
            sector_name, nrows = calculate_sector_scores_for_sector(args)
            print(f"[SECTOR] Done: {sector_name} - {nrows} sector score rows processed.")
        except Exception as e:
            print(f"[SECTOR] Error calculating sector scores for {sector}: {e}")

def main():
    print("=== Processing sector scores sector-by-sector ===")
    process_sector_scores_sector_by_sector()
    print("\n=== Sector score calculation complete ===")

if __name__ == "__main__":
    main() 