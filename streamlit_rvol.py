import streamlit as st
import plotly.graph_objs as go
import pandas as pd
import os
from dotenv import load_dotenv
import pytz
from streamlit_autorefresh import st_autorefresh
import json
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import concurrent.futures

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Helper: connect to Supabase
@st.cache_resource
def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# Helper: load asset/sector mappings
@st.cache_data
def load_asset_category_map():
    with open("asset_category_map.json", "r") as f:
        return json.load(f)

@st.cache_data
def load_asset_etf_map():
    with open("asset_etf_map.json", "r") as f:
        return json.load(f)

asset_category_map = load_asset_category_map()
asset_etf_map = load_asset_etf_map()
ticker_to_sector = {ticker: sector for sector, tickers in asset_category_map.items() for ticker in tickers}

# Helper: get current UTC time
@st.cache_data(show_spinner=False)
def get_current_utc_time():
    return datetime.now(timezone.utc)

# Helper: get cutoff date for 2 weeks before a given reference date (UTC)
def get_2wk_ago_utc_from(ref_date):
    two_weeks_ago = ref_date - timedelta(days=14)
    return two_weeks_ago

# Fetch last 2 weeks of RVol data for a ticker (filter in DB)
@st.cache_data(show_spinner=False)
def fetch_2wk_rvol_data(ticker, ref_date):
    print(f"[FETCH] Starting fetch for asset: {ticker}")
    print(f"[DEBUG] ref_date: {ref_date}")
    supabase = get_supabase_client()
    cutoff = get_2wk_ago_utc_from(ref_date)
    print(f"[DEBUG] cutoff: {cutoff}")

    # Debug: Check whats in the database for this ticker
    try:
        debug_response = supabase.table("rvol_data").select("ticker, date, rvol").eq(ticker, ticker).limit(5).execute()
        print(f"[DEBUG] Sample data for {ticker}: {debug_response.data}")
    except Exception as e:
        print(f"[DEBUG] Error checking sample data for {ticker}: {e}")

    response = (
        supabase.table("rvol_data")
        .select("ticker, name, date, rvol")
        .eq(ticker, ticker)
        .gte("date", cutoff.strftime("%Y-%m-%dT%H:%M:%S"))
        .lte("date", ref_date.strftime("%Y-%m-%dT%H:%M:%S"))
        .order("date", desc=True)
        .limit(336)  # 14 days * 24 hours
        .execute()
    )
    print(f"[DEBUG] Raw response for {ticker}: {len(response.data)} rows)  if response.data:
        print(f"[DEBUG] First row: {response.data[0]}")
        print(f"[DEBUG] Last row: {response.data[-1]}")

    df = pd.DataFrame(response.data)
    if df.empty:
        print(f"[FETCH] No data for asset: {ticker}")
        return df

    print(f"[DEBUG] DataFrame created with {len(df)} rows")
    print(f"[DEBUG] DataFrame columns: {df.columns.tolist()}")
    print(f"[DEBUG] Sample dates before conversion: {df['date'].head().tolist()}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    print(f"[DEBUG] After datetime conversion: {len(df)} rows")
    print(f"[DEBUG] Sample dates after conversion: {df['date'].head().tolist()}")

    df = df[(df["date"] >= cutoff) & (df["date"] <= ref_date)]
    print(f"[DEBUG] After date filtering: {len(df)} rows")

    # Convert to GMT+3
    tz = pytz.timezone('Etc/GMT-3')
        df["date_gmt3"] = df["date"].dt.tz_convert(tz)
    print(f"[DEBUG] After timezone conversion: {len(df)} rows")

    df = df.dropna(subset=["date_gmt3", "rvol"])
    print(f"[DEBUG] After dropna: {len(df)} rows")

    df["rvol"] = df["rvol"].astype(float)
    print(f"[FETCH] Finished fetch for asset: {ticker} | min date: {df['date'].min()} | max date: {df['date'].max()} | rows: {len(df)}")
    return df

# Fetch last 2 weeks of sector score data for a sector (filter in DB)
@st.cache_data(show_spinner=False)
def fetch_2wk_sector_score_data(sector, ref_date):
    print(f"[FETCH] Starting fetch for sector: {sector}")
    print(f"[DEBUG] ref_date: {ref_date}")
    supabase = get_supabase_client()
    cutoff = get_2wk_ago_utc_from(ref_date)
    print(f"[DEBUG] cutoff: {cutoff}")

    # Debug: Check whats in the database for this sector
    try:
        debug_response = supabase.table("sector_score_data").select("sector, date, hour, sector_score").eq(sector, sector).limit(5).execute()
        print(f"[DEBUG] Sample data for {sector}: {debug_response.data}")
    except Exception as e:
        print(f"[DEBUG] Error checking sample data for {sector}: {e}")

    response = (
        supabase.table("sector_score_data")
        .select("sector, date, hour, sector_score")
        .eq(sector, sector)
        .gte("date", cutoff.strftime("%Y-%m-%dT%H:%M:%S"))
        .lte("date", ref_date.strftime("%Y-%m-%dT%H:%M:%S"))
        .order("date", desc=True)
        .limit(336)  # 14 days * 24 hours
        .execute()
    )
    print(f"[DEBUG] Raw response for {sector}: {len(response.data)} rows)  if response.data:
        print(f"[DEBUG] First row: {response.data[0]}")
        print(f"[DEBUG] Last row: {response.data[-1]}")

    df = pd.DataFrame(response.data)
    if df.empty:
        print(f"[FETCH] No data for sector: {sector}")
        return df

    print(f"[DEBUG] DataFrame created with {len(df)} rows")
    print(f"[DEBUG] DataFrame columns: {df.columns.tolist()}")
    print(f"[DEBUG] Sample dates before conversion: {df['date'].head().tolist()}")

    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    print(f"[DEBUG] After datetime conversion: {len(df)} rows")
    print(f"[DEBUG] Sample dates after conversion: {df['date'].head().tolist()}")

    df = df[(df["date"] >= cutoff) & (df["date"] <= ref_date)]
    print(f"[DEBUG] After date filtering: {len(df)} rows")

    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
    print(f"[DEBUG] After hour conversion: {len(df)} rows")

    df = df.dropna(subset=["hour", "sector_score"])
    print(f"[DEBUG] After dropna: {len(df)} rows")

    df["sector_score"] = df["sector_score"].astype(float)
    print(f"[FETCH] Finished fetch for sector: {sector} | min date: {df['date'].min()} | max date: {df['date'].max()} | rows: {len(df)}")
    return df

# Get the latest full day (00:00 to 23:00) for a ticker in GMT+3
def get_latest_full_day(df):
    if df.empty:
        return pd.DataFrame(), None
    df["hour"] = df["date_gmt3"].dt.hour
    df["day"] = df["date_gmt3"].dt.date
    # Find all days with a 00:00 entry
    days_with_midnight = df[df["hour"] == 0]["day"].unique()
    full_days = []
    for day in days_with_midnight:
        hours = set(df[df["day"] == day]["hour"].unique())
        if set(range(24)).issubset(hours):
            full_days.append(day)
    if not full_days:
        return pd.DataFrame(), None
    latest_day = max(full_days)
    day_df = df[df["day"] == latest_day].copy()
    return day_df, latest_day

# Plot RVol chart for a single asset
def plot_asset_rvol_chart(day_df, asset_label, latest_day, rvol_2yr_df=None):
    if day_df.empty:
        st.warning(f"No data for {asset_label} on the latest full day.")
        return
    hour_rvol = day_df.groupby("hour")["rvol"].mean().reset_index()
    percentile_70 = None
    if rvol_2yr_df is not None and not rvol_2yr_df.empty:
        percentile_70 = rvol_2yr_df["rvol"].quantile(0.7)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=hour_rvol["hour"], y=hour_rvol["rvol"], name="RVol", marker_color="blue"))
    if percentile_70 is not None:
        fig.add_hline(y=percentile_70, line_width=3, line_dash="dash", line_color="red", annotation_text="70th percentile", annotation_position="top right")
    fig.update_layout(
        title=f"{asset_label} — {latest_day}",
        xaxis_title="Hour of Day (GMT+3)",
        yaxis_title="RVol",
        xaxis=dict(tickmode='array', tickvals=hour_rvol["hour"].tolist(), ticktext=[str(h) for h in hour_rvol["hour"]]),
        yaxis=dict(rangemode="tozero"),
        height=300
    )
    st.plotly_chart(fig, use_container_width=True)

# Get the latest full day for sector score data (by date)
def get_latest_full_day_sector(df):
    if df.empty:
        return pd.DataFrame(), None
    df["date_only"] = df["date"].dt.date
    days_with_midnight = df[df["hour"] == 0]["date_only"].unique()
    full_days = []
    for day in days_with_midnight:
        hours = set(df[df["date_only"] == day]["hour"].unique())
        if set(range(24)).issubset(hours):
            full_days.append(day)
    if not full_days:
        return pd.DataFrame(), None
    latest_day = max(full_days)
    day_df = df[df["date_only"] == latest_day].copy()
    return day_df, latest_day

# Plot sector score chart
def plot_sector_score_chart(sector_score_df, sector_label, latest_day):
    if sector_score_df.empty:
        st.warning(f"No sector score data for {sector_label} on the latest full day.")
        return
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=sector_score_df["hour"],
        y=sector_score_df["sector_score"],
        name="Sector Score",
        marker_color="gold"
    ))
    fig.update_layout(
        title=f"{sector_label} — {latest_day} (Sector Score)",
        xaxis_title="Hour of Day (GMT+3)",
        yaxis_title="Sector Score",
        xaxis=dict(tickmode='array', tickvals=sector_score_df["hour"].tolist(), ticktext=[str(h) for h in sector_score_df["hour"]]),
        yaxis=dict(rangemode="tozero"),
        height=300
    )
    st.plotly_chart(fig, use_container_width=True)

# --- Batch fetch helpers ---
@st.cache_data(show_spinner=False)
def batch_fetch_2wk_rvol_data(ticker_list, ref_date):
    print(f"[BATCH] Starting batch fetch for {len(ticker_list)} assets...")
    def fetch_one(ticker):
        try:
            return ticker, fetch_2wk_rvol_data(ticker, ref_date)
        except Exception as e:
            print(f"[ERROR] Fetch failed for asset: {ticker} | {e}")
            return ticker, pd.DataFrame()
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_ticker = {executor.submit(fetch_one, ticker): ticker for ticker in ticker_list}
        for future in concurrent.futures.as_completed(future_to_ticker):
            ticker, df = future.result()
            results[ticker] = df
    print(f"[BATCH] Finished batch fetch for assets.")
    return results

@st.cache_data(show_spinner=False)
def batch_fetch_2wk_sector_score_data(sector_list, ref_date):
    print(f"[BATCH] Starting batch fetch for {len(sector_list)} sectors...")
    def fetch_one(sector):
        try:
            return sector, fetch_2wk_sector_score_data(sector, ref_date)
        except Exception as e:
            print(f"[ERROR] Fetch failed for sector: {sector} | {e}")
            return sector, pd.DataFrame()
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        future_to_sector = {executor.submit(fetch_one, sector): sector for sector in sector_list}
        for future in concurrent.futures.as_completed(future_to_sector):
            sector, df = future.result()
            results[sector] = df
    print(f"[BATCH] Finished batch fetch for sectors.")
    return results

# Main dashboard
def main():
    st_autorefresh(interval=3600000, key="rvol_autorefresh")
    st.title("RVol Dashboard — Latest Full Day (GMT+3)")
    if st.button("Refresh Data Now"):
        st.rerun()
    all_tickers = sorted(ticker_to_sector.keys())
    all_sectors = sorted(set(ticker_to_sector.values()))
    # Use current UTC time as reference for the 2-week window
    ref_date = get_current_utc_time()
    st.info(f"Reference time for data window (UTC): {ref_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    # Add global loading spinner for batch fetch
    with st.spinner("Loading all asset and sector data in parallel. This may take a few seconds..."):
        rvol_data_dict = batch_fetch_2wk_rvol_data(all_tickers, ref_date)
        sector_score_data_dict = batch_fetch_2wk_sector_score_data(all_sectors, ref_date)
    for selected_asset in all_tickers:
        asset_label = f"{selected_asset} ({ticker_to_sector[selected_asset]})"
        df = rvol_data_dict.get(selected_asset, pd.DataFrame())
        if not df.empty:
            st.caption(f"{asset_label}: min date_gmt3 = {df['date_gmt3'].min()}, max date_gmt3 = {df['date_gmt3'].max()}")
        day_df, latest_day = get_latest_full_day(df)
        if latest_day is not None:
            st.success(f"Showing data for {latest_day} (GMT+3)")
            st.caption(f"Hours present for {asset_label} on {latest_day}: {sorted(day_df['hour'].unique())}")
        else:
            st.warning(f"No full day of data (00:00-23:00) for {asset_label}.")
        plot_asset_rvol_chart(day_df, asset_label, latest_day, rvol_2yr_df=df)
        sector = ticker_to_sector.get(selected_asset)
        if sector and latest_day is not None:
            sector_score_df_all = sector_score_data_dict.get(sector, pd.DataFrame())
            sector_score_day_df, sector_score_latest_day = get_latest_full_day_sector(sector_score_df_all)
            if sector_score_latest_day == latest_day:
                plot_sector_score_chart(sector_score_day_df, sector, latest_day)
            else:
                st.warning(f"No sector score data for {sector} on {latest_day}.")
        st.markdown('---')

if __name__ == "__main__":
    main() 