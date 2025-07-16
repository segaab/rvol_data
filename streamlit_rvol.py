import streamlit as st
import plotly.graph_objs as go
import pandas as pd
from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pytz
from streamlit_autorefresh import st_autorefresh
import json

# Load environment variables
load_dotenv()

SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"

@st.cache_resource
def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_latest_day_rvol_data():
    supabase = get_supabase_client()
    response = supabase.table("rvol_data").select("ticker, name, date, rvol").order("date", desc=True).execute()
    df = pd.DataFrame(response.data)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "rvol"])
    df["rvol"] = df["rvol"].astype(float)
    # For each ticker, keep only the latest day
    latest_days = df.groupby("ticker")["date"].transform("max").dt.date
    df = df[df["date"].dt.date == latest_days]
    return df

def load_asset_etf_map():
    with open("asset_etf_map.json", "r") as f:
        return json.load(f)

asset_etf_map = load_asset_etf_map()

def plot_asset_histogram(day_df, asset_label, latest_day, etf_rvols=None, filter_mode="All Activity", rvol_threshold=None):
    tz = pytz.timezone('Etc/GMT-3')  # GMT+3
    if day_df["date"].dt.tz is None:
        day_df["date_gmt3"] = day_df["date"].dt.tz_localize('UTC').dt.tz_convert(tz)
    else:
        day_df["date_gmt3"] = day_df["date"].dt.tz_convert(tz)
    day_df["hour"] = day_df["date_gmt3"].dt.hour
    day_df = day_df.dropna(subset=["rvol"])
    hour_rvol = day_df.groupby("hour")["rvol"].mean().reset_index()
    rvols = hour_rvol["rvol"]
    percentile_70 = rvols.quantile(0.7) if not rvols.empty else None
    if rvol_threshold is None:
        rvol_threshold = percentile_70
    st.write(f"### {asset_label} — {latest_day}")
    st.write(f"70th percentile RVol: {percentile_70:.2f}" if percentile_70 is not None else "No RVol data.")
    # Highlight logic
    highlight_hours = []
    if filter_mode != "All Activity" and etf_rvols is not None and not hour_rvol.empty:
        for _, row in hour_rvol.iterrows():
            hour = row["hour"]
            asset_val = row["rvol"]
            etf_vals = [etf_rvol.get(hour, 0) for etf_rvol in etf_rvols]
            if filter_mode == "Sector Activity":
                if asset_val >= rvol_threshold and any(etf >= rvol_threshold for etf in etf_vals):
                    highlight_hours.append(hour)
            elif filter_mode == "Asset-Specific Activity":
                if asset_val >= rvol_threshold and all(etf < rvol_threshold for etf in etf_vals):
                    highlight_hours.append(hour)
    # Plot
    fig = go.Figure()
    bar_colors = ["orange" if h in highlight_hours else "blue" for h in hour_rvol["hour"]]
    fig.add_trace(go.Bar(x=hour_rvol["hour"], y=hour_rvol["rvol"], name="RVol", marker_color=bar_colors))
    fig.add_hline(y=percentile_70, line_width=3, line_dash="dash", line_color="red", annotation_text="70th percentile", annotation_position="top right")
    if not hour_rvol.empty:
        fig.update_layout(
            title=f"RVol Histogram",
            xaxis_title="Hour of Day (GMT+3)",
            yaxis_title="RVol",
            xaxis=dict(
                tickmode='array',
                tickvals=hour_rvol["hour"].tolist(),
                ticktext=[str(h) for h in hour_rvol["hour"]]
            )
        )
    else:
        fig.update_layout(title=f"RVol Histogram", xaxis_title="Hour of Day (GMT+3)", yaxis_title="RVol")
    st.plotly_chart(fig, use_container_width=True)

# Add static asset list (TICKER_MAP)
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

def main():
    st_autorefresh(interval=3600000, key="rvol_autorefresh")
    st.title("RVol Histogram for All Assets (Latest Day)")
    filter_mode = st.sidebar.selectbox("Activity Filter", ["All Activity", "Sector Activity", "Asset-Specific Activity"])
    df = fetch_latest_day_rvol_data()
    if df.empty:
        st.warning("No RVol data available.")
    for name, ticker in TICKER_MAP.items():
        asset_label = f"{name} ({ticker})"
        asset_df = df[df["ticker"] == ticker] if not df.empty else pd.DataFrame()
        if asset_df.empty:
            st.write(f"### {asset_label}")
            st.warning("No data for this asset.")
            continue
        latest_day = asset_df["date"].dt.date.max()
        day_df = asset_df[asset_df["date"].dt.date == latest_day].copy()
        if day_df.empty:
            st.write(f"### {asset_label}")
            st.warning("No data for the latest day for this asset.")
            continue
        # Prepare ETF RVols for this asset
        etf_rvols = []
        if ticker in asset_etf_map:
            for etf in asset_etf_map[ticker]:
                etf_df = df[df["ticker"] == etf]
                if not etf_df.empty:
                    etf_day_df = etf_df[etf_df["date"].dt.date == latest_day].copy()
                    etf_day_df = etf_day_df.dropna(subset=["rvol"])
                    etf_hour_rvol = etf_day_df.groupby("hour")["rvol"].mean().to_dict()
                    etf_rvols.append(etf_hour_rvol)
        plot_asset_histogram(day_df, asset_label, latest_day, etf_rvols=etf_rvols, filter_mode=filter_mode)

if __name__ == "__main__":
    main() 