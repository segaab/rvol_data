import streamlit as st
import plotly.graph_objs as go
import pandas as pd
# from supabase import create_client, Client
import os
from dotenv import load_dotenv
import pytz
from streamlit_autorefresh import st_autorefresh
import json
import psycopg2
from psycopg2.extras import RealDictCursor

# Load environment variables
load_dotenv()

# SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
# SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"

# @st.cache_resource
# def get_supabase_client() -> Client:
#     return create_client(SUPABASE_URL, SUPABASE_KEY)

# def fetch_latest_day_rvol_data():
#     supabase = get_supabase_client()
#     response = supabase.table("rvol_data").select("ticker, name, date, rvol").order("date", desc=True).execute()
#     df = pd.DataFrame(response.data)
#     if df.empty:
#         return df
#     df["date"] = pd.to_datetime(df["date"], errors="coerce")
#     df = df.dropna(subset=["date", "rvol"])
#     df["rvol"] = df["rvol"].astype(float)
#     # For each ticker, keep only the latest day
#     latest_days = df.groupby("ticker")["date"].transform("max").dt.date
#     df = df[df["date"].dt.date == latest_days]
#     return df

# def fetch_latest_sector_scores(sector, latest_day):
#     supabase = get_supabase_client()
#     response = supabase.table("sector_score_data").select("sector, date, hour, sector_score").eq("sector", sector).eq("date", str(latest_day)).order("hour").execute()
#     df = pd.DataFrame(response.data)
#     if df.empty:
#         return df
#     df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
#     df = df.dropna(subset=["hour", "sector_score"])
#     df["sector_score"] = df["sector_score"].astype(float)
#     return df

# def fetch_2yr_rvol_data(ticker):
#     supabase = get_supabase_client()
#     response = supabase.table("rvol_data").select("ticker, date, rvol").eq("ticker", ticker).order("date", desc=True).limit(17520).execute()  # 2 years * 365 * 24 = 17520
#     df = pd.DataFrame(response.data)
#     if df.empty:
#         return df
#     df["date"] = pd.to_datetime(df["date"], errors="coerce")
#     # Convert to GMT+3
#     tz = pytz.timezone('Etc/GMT-3')
#     if df["date"].dt.tz is None:
#         df["date_gmt3"] = df["date"].dt.tz_localize('UTC').dt.tz_convert(tz)
#     else:
#         df["date_gmt3"] = df["date"].dt.tz_convert(tz)
#     df["rvol"] = pd.to_numeric(df["rvol"], errors="coerce")
#     df = df.dropna(subset=["rvol", "date_gmt3"])
#     return df

# def fetch_latest_full_day_rvol_data():
#     supabase = get_supabase_client()
#     response = supabase.table("rvol_data").select("ticker, name, date, rvol").order("date", desc=True).limit(17520).execute()
#     df = pd.DataFrame(response.data)
#     if df.empty:
#         return df
#     df["date"] = pd.to_datetime(df["date"], errors="coerce")
#     # Convert to GMT+3
#     tz = pytz.timezone('Etc/GMT-3')
#     if df["date"].dt.tz is None:
#         df["date_gmt3"] = df["date"].dt.tz_localize('UTC').dt.tz_convert(tz)
#     else:
#         df["date_gmt3"] = df["date"].dt.tz_convert(tz)
#     df = df.dropna(subset=["date_gmt3", "rvol"])
#     df["rvol"] = df["rvol"].astype(float)
#     # For each ticker, keep only the latest day that has all 24 hours (00:00 to 23:00) in GMT+3
#     latest_full_days = {}
#     for ticker in df["ticker"].unique():
#         ticker_df = df[df["ticker"] == ticker].copy()
#         ticker_df["hour"] = ticker_df["date_gmt3"].dt.hour
#         ticker_df["day"] = ticker_df["date_gmt3"].dt.date
#         # Find all days with a 00:00 entry
#         days_with_midnight = ticker_df[ticker_df["hour"] == 0]["day"].unique()
#         # For each such day, check if all 24 hours are present
#         full_days = []
#         for day in days_with_midnight:
#             hours = set(ticker_df[ticker_df["day"] == day]["hour"].unique())
#             if set(range(24)).issubset(hours):
#                 full_days.append(day)
#         if full_days:
#             latest_day = max(full_days)
#             latest_full_days[ticker] = latest_day
#     # Filter df to only include rows for the latest full day for each ticker (in GMT+3)
#     mask = df.apply(lambda row: row["date_gmt3"].date() == latest_full_days.get(row["ticker"]) if row["ticker"] in latest_full_days else False, axis=1)
#     return df[mask]

# Helper to map ticker to sector
with open("asset_category_map.json", "r") as f:
    asset_category_map = json.load(f)
ticker_to_sector = {ticker: sector for sector, tickers in asset_category_map.items() for ticker in tickers}

def load_asset_etf_map():
    with open("asset_etf_map.json", "r") as f:
        return json.load(f)

asset_etf_map = load_asset_etf_map()

# Add a variable for the high activity percentile threshold
HIGH_ACTIVITY_PERCENTILE = 0.7  # Can be adjusted in the future

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

def plot_bar_chart_with_anomaly(
    df,
    label,
    latest_day,
    value_col,
    value_2yr_df=None,
    percentile=0.7,
    filter_mode="All Activity",
    yaxis_label=None,
    bar_color_logic=None,
    chart_title=None,
    height=300
):
    if df.empty:
        st.info(f"No {label} data available.")
        return
    # Calculate percentile threshold from 2yr data if available
    percentile_val = None
    if value_2yr_df is not None and not value_2yr_df.empty:
        percentile_val = value_2yr_df[value_col].quantile(percentile)
    # Bar color logic
    bar_colors = []
    for _, row in df.iterrows():
        if bar_color_logic is not None:
            bar_colors.append(bar_color_logic(row, percentile_val))
        else:
            # Default: highlight above percentile
            if percentile_val is not None and row[value_col] >= percentile_val:
                bar_colors.append("red")
            else:
                bar_colors.append("gold")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=df["hour"],
        y=df[value_col],
        name=label,
        marker_color=bar_colors
    ))
    # Add percentile line if available
    if percentile_val is not None:
        fig.add_hline(y=percentile_val, line_width=3, line_dash="dash", line_color="yellow", annotation_text=f"{int(percentile*100)}th percentile (2yr)", annotation_position="top right")
    # Calculate y-axis max with 40% expansion
    y_max = df[value_col].max() if not df[value_col].empty else 1
    yaxis_range = [0, y_max * 1.4]
    fig.update_layout(
        title=chart_title or f"{label} — {latest_day}",
        xaxis=dict(
            title="Hour of Day (GMT+3)",
            tickmode='array',
            tickvals=df["hour"].tolist(),
            ticktext=[str(h) for h in df["hour"]]
        ),
        yaxis=dict(
            title=yaxis_label or label,
            range=yaxis_range,
            rangemode="tozero"
        ),
        bargap=0.1,
        height=height
    )
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

# --- POSTGRESQL DATA LOADING ---
def get_db_conn():
    SUPABASE_DB_URL = os.getenv("SUPABASE_DB_URL")
    return psycopg2.connect(SUPABASE_DB_URL, cursor_factory=RealDictCursor)

def fetch_latest_full_day_rvol_data():
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticker, name, date, rvol
                FROM rvol_data
                ORDER BY date DESC
                LIMIT 17520
            """)
            rows = cur.fetchall()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    tz = pytz.timezone('Etc/GMT-3')
    if df["date"].dt.tz is None:
        df["date_gmt3"] = df["date"].dt.tz_localize('UTC').dt.tz_convert(tz)
    else:
        df["date_gmt3"] = df["date"].dt.tz_convert(tz)
    df = df.dropna(subset=["date_gmt3", "rvol"])
    df["rvol"] = df["rvol"].astype(float)
    latest_full_days = {}
    for ticker in df["ticker"].unique():
        ticker_df = df[df["ticker"] == ticker].copy()
        ticker_df["hour"] = ticker_df["date_gmt3"].dt.hour
        ticker_df["day"] = ticker_df["date_gmt3"].dt.date
        days_with_midnight = ticker_df[ticker_df["hour"] == 0]["day"].unique()
        full_days = []
        for day in days_with_midnight:
            hours = set(ticker_df[ticker_df["day"] == day]["hour"].unique())
            if set(range(24)).issubset(hours):
                full_days.append(day)
        if full_days:
            latest_day = max(full_days)
            latest_full_days[ticker] = latest_day
    mask = df.apply(lambda row: row["date_gmt3"].date() == latest_full_days.get(row["ticker"]) if row["ticker"] in latest_full_days else False, axis=1)
    return df[mask]

def fetch_2yr_rvol_data(ticker):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT ticker, date, rvol
                FROM rvol_data
                WHERE ticker = %s
                ORDER BY date DESC
                LIMIT 17520
            """, (ticker,))
            rows = cur.fetchall()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    tz = pytz.timezone('Etc/GMT-3')
    if df["date"].dt.tz is None:
        df["date_gmt3"] = df["date"].dt.tz_localize('UTC').dt.tz_convert(tz)
    else:
        df["date_gmt3"] = df["date"].dt.tz_convert(tz)
    df["rvol"] = pd.to_numeric(df["rvol"], errors="coerce")
    df = df.dropna(subset=["rvol", "date_gmt3"])
    return df

def fetch_latest_sector_scores(sector, latest_day):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sector, date, hour, sector_score
                FROM sector_score_data
                WHERE sector = %s AND date = %s
                ORDER BY hour
            """, (sector, str(latest_day)))
            rows = cur.fetchall()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["hour"] = pd.to_numeric(df["hour"], errors="coerce")
    df = df.dropna(subset=["hour", "sector_score"])
    df["sector_score"] = df["sector_score"].astype(float)
    return df

def fetch_2yr_sector_score_data(sector):
    with get_db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT sector, date, hour, sector_score
                FROM sector_score_data
                WHERE sector = %s
                ORDER BY date DESC, hour
                LIMIT 17520
            """, (sector,))
            rows = cur.fetchall()
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["sector_score"] = pd.to_numeric(df["sector_score"], errors="coerce")
    df = df.dropna(subset=["sector_score"])
    return df

def main():
    st_autorefresh(interval=3600000, key="rvol_autorefresh")
    st.title("RVol & Sector Score Charts for All Assets (Latest Day)")
    filter_mode = st.sidebar.selectbox(
        "Activity Filter",
        ["All Activity", "Low Activity", "Sector Anomaly", "Asset Anomaly"]
    )
    df = fetch_latest_full_day_rvol_data()
    if df.empty:
        st.warning("No RVol data available.")
    # Asset RVol bar color logic
    def asset_bar_color_logic(row, percentile_val):
        asset_high = percentile_val is not None and row["rvol"] >= percentile_val
        sector_high = False
        if "sector_score" in row and pd.notna(row["sector_score"]) and percentile_val is not None:
            sector_high = row["sector_score"] >= percentile_val
        if filter_mode == "Low Activity":
            if sector_high and not asset_high:
                return "blue"
            else:
                return "lightgray"
        elif filter_mode == "Sector Anomaly":
            if sector_high and asset_high:
                return "red"
            else:
                return "lightgray"
        elif filter_mode == "Asset Anomaly":
            if not sector_high and asset_high:
                return "orange"
            else:
                return "lightgray"
        else:
            return "black"

    # Sector score bar color logic (default: red if above percentile, gold otherwise)
    def sector_bar_color_logic(row, percentile_val):
        if percentile_val is not None and row["sector_score"] >= percentile_val:
            return "red"
        else:
            return "gold"

    for name, ticker in TICKER_MAP.items():
        asset_label = f"{name} ({ticker})"
        asset_df = df[df["ticker"] == ticker] if not df.empty else pd.DataFrame()
        if asset_df.empty:
            st.write(f"### {asset_label}")
            st.warning("No data for this asset.")
            continue
        latest_day = asset_df["date_gmt3"].dt.date.max()
        day_df = asset_df[asset_df["date_gmt3"].dt.date == latest_day].copy()
        if day_df.empty:
            st.write(f"### {asset_label}")
            st.warning("No data for the latest day for this asset.")
            continue
        day_df["hour"] = day_df["date_gmt3"].dt.hour  # Ensure 'hour' column exists
        sector = ticker_to_sector.get(ticker)
        sector_score_df = fetch_latest_sector_scores(sector, latest_day) if sector else pd.DataFrame()
        rvol_2yr_df = fetch_2yr_rvol_data(ticker)
        sector_score_2yr_df = fetch_2yr_sector_score_data(sector) if sector else None
        # Asset RVol chart
        merged = day_df.groupby("hour")["rvol"].mean().reset_index()
        if sector_score_df is not None and not sector_score_df.empty:
            merged = pd.merge(merged, sector_score_df[["hour", "sector_score"]], on="hour", how="left")
        plot_bar_chart_with_anomaly(
            merged,
            label="Asset RVol",
            latest_day=latest_day,
            value_col="rvol",
            value_2yr_df=rvol_2yr_df,
            percentile=HIGH_ACTIVITY_PERCENTILE,
            filter_mode=filter_mode,
            yaxis_label="RVol",
            bar_color_logic=asset_bar_color_logic,
            chart_title=f"{asset_label} — {latest_day} (RVol)",
            height=300
        )
        # Sector Score chart
        plot_bar_chart_with_anomaly(
            sector_score_df,
            label="Sector Score",
            latest_day=latest_day,
            value_col="sector_score",
            value_2yr_df=sector_score_2yr_df,
            percentile=HIGH_ACTIVITY_PERCENTILE,
            filter_mode=filter_mode,
            yaxis_label="Sector Score",
            bar_color_logic=sector_bar_color_logic,
            chart_title=f"{asset_label} — {latest_day} (Sector Score)",
            height=300
        )

if __name__ == "__main__":
    main() 