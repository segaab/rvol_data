import json
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL") or ""
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or ""

def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def load_json(filename):
    with open(filename, "r") as f:
        return json.load(f)

def fetch_last_24_hours_rvol_data(supabase):
    response = supabase.table("rvol_data").select("ticker, date, rvol").order("date", desc=True).limit(10000).execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return []
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "rvol"])
    df["rvol"] = df["rvol"].astype(float)
    latest_day = df["date"].dt.date.max()
    day_df = df[df["date"].dt.date == latest_day].copy()
    if day_df.empty:
        return []
    hours_series = day_df["date"].dt.hour
    sorted_hours = sorted(hours_series.unique())
    if len(sorted_hours) < 24:
        return []
    hours_to_process = sorted_hours[-24:]
    hour_dfs = []
    for hour in hours_to_process:
        hour_df = day_df[hours_series == hour].copy()
        hour_dfs.append((hour_df, latest_day, hour))
    return hour_dfs

def calculate_and_insert_latest_sector_scores(hour_df, latest_day, latest_complete_hour, supabase):
    asset_category_map = load_json("asset_category_map.json")
    asset_etf_map = load_json("asset_etf_map.json")
    for sector, assets in asset_category_map.items():
        etf_symbol = asset_etf_map[assets[0]][0] if assets[0] in asset_etf_map else None
        sector_assets_df = hour_df[hour_df["ticker"].isin(assets)]
        if sector_assets_df.empty or not etf_symbol:
            continue
        mean_asset_rvol = sector_assets_df["rvol"].mean()
        etf_rvol = None
        etf_df = hour_df[hour_df["ticker"] == etf_symbol]
        if not etf_df.empty:
            etf_rvol = etf_df["rvol"].mean()
        if etf_rvol is not None and not pd.isna(mean_asset_rvol):
            sector_score = 0.4 * etf_rvol + 0.6 * mean_asset_rvol
            record = {
                "sector": sector,
                "date": str(latest_day),
                "hour": int(latest_complete_hour),
                "etf_rvol": float(etf_rvol),
                "mean_asset_rvol": float(mean_asset_rvol),
                "sector_score": float(sector_score)
            }
            try:
                supabase.table("sector_score_data").insert(record).execute()
                print(f"Inserted sector score for {sector} {latest_day} hour {latest_complete_hour}: {sector_score}")
            except Exception as e:
                if "duplicate key value violates unique constraint" in str(e):
                    print(f"Duplicate sector score for {sector} {latest_day} hour {latest_complete_hour}, skipping.")
                else:
                    print(f"Error inserting sector score for {sector} {latest_day} hour {latest_complete_hour}: {e}")

def main():
    supabase = get_supabase_client()
    hour_dfs = fetch_last_24_hours_rvol_data(supabase)
    if hour_dfs:
        for hour_df, latest_day, hour in hour_dfs:
            if hour_df is not None and isinstance(hour_df, pd.DataFrame) and not hour_df.empty:
                calculate_and_insert_latest_sector_scores(hour_df, latest_day, hour, supabase)
    else:
        print("No complete hour data available for sector score update.")

if __name__ == "__main__":
    main() 