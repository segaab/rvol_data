import streamlit as st
import pandas as pd
from yahooquery import Ticker
from datetime import timedelta, datetime
import json

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

DAYS = 730  # 2 years
ROLLING_WINDOW = 120

# Remove ^N225 and DX-Y.NYB from TICKER_MAP and ETF_MAP
TICKER_MAP = {k: v for k, v in TICKER_MAP.items() if v not in ["^N225", "DX-Y.NYB"]}
ETF_MAP = {k: v for k, v in ETF_MAP.items() if k not in ["^N225", "DX-Y.NYB"] and v[0] not in ["^N225", "DX-Y.NYB"]}

# All unique asset and ETF symbols
symbols = list(set(TICKER_MAP.values()) | set(v[0] for v in ETF_MAP.values()))

# Separate asset and ETF symbols
asset_symbols = [v for v in TICKER_MAP.values() if v not in [etf[0] for etf in ETF_MAP.values()]]
etf_symbols = list(set(v[0] for v in ETF_MAP.values()))

# Load sector mapping from asset_category_map.json
with open("asset_category_map.json", "r") as f:
    ASSET_CATEGORY_MAP = json.load(f)
# Build a reverse mapping: asset -> sector
ASSET_TO_SECTOR = {asset: sector for sector, assets in ASSET_CATEGORY_MAP.items() for asset in assets}
# Build a reverse mapping: ticker -> name
TICKER_TO_NAME = {v: k for k, v in TICKER_MAP.items()}

st.title("RVol Monitor")
if st.button("Rerun"):
    st.rerun()

# Add Streamlit controls for gap up detection
st.sidebar.header("Gap Up RVol Filter")
market_open = st.sidebar.selectbox(
    "Select Market Open Window:",
    ["London (10:00-11:00)", "NY (16:00-17:00)", "Asian (3:00-4:00)"]
)
gap_threshold = st.sidebar.number_input(
    "Gap Up Threshold (ratio, e.g. 1.5 = 50% higher)", min_value=1.0, max_value=10.0, value=1.5, step=0.1
)

# Determine which hours to use for market open
if market_open.startswith("London"):
    open_hours = [10, 11]
elif market_open.startswith("NY"):
    open_hours = [16, 17]
elif market_open.startswith("Asian"):
    open_hours = [3, 4]
else:
    open_hours = [16, 17]

def detect_gap_up(df, open_hours, threshold):
    if df.empty:
        return False, None, None
    df = df.copy()
    df["datetime_gmt3_dt"] = pd.to_datetime(df["datetime_gmt3"], errors="coerce")
    df = df.dropna(subset=["datetime_gmt3_dt"])
    df = df.sort_values("datetime_gmt3_dt", ascending=False)
    df["date_gmt3"] = df["datetime_gmt3_dt"].dt.date
    df["hour_gmt3"] = df["datetime_gmt3_dt"].dt.hour
    if df.empty:
        return False, None, None
    latest_day = df.iloc[0]["date_gmt3"]
    prev_day = latest_day - pd.Timedelta(days=1)
    # Get rvol for open hours for both days
    curr_open = df[(df["date_gmt3"] == latest_day) & (df["hour_gmt3"].isin(open_hours))]["rvol"]
    prev_open = df[(df["date_gmt3"] == prev_day) & (df["hour_gmt3"].isin(open_hours))]["rvol"]
    if curr_open.empty or prev_open.empty:
        return False, None, None
    curr_mean = curr_open.mean()
    prev_mean = prev_open.mean()
    if prev_mean == 0 or pd.isna(prev_mean):
        return False, curr_mean, prev_mean
    gap_ratio = curr_mean / prev_mean
    print(f"Asian Open Debug: curr_open={curr_open}, prev_open={prev_open}, curr_mean={curr_mean}, prev_mean={prev_mean}, gap_ratio={gap_ratio}")
    return gap_ratio >= threshold, curr_mean, prev_mean

@st.cache_data(show_spinner=True)
def fetch_rvol_data(symbol):
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
        return hist
    else:
        return pd.DataFrame()

# Fetch ETF data in the background (not displayed)
@st.cache_data(show_spinner=False)
def fetch_all_etf_data():
    etf_data = {}
    for symbol in etf_symbols:
        etf_data[symbol] = fetch_rvol_data(symbol)
    return etf_data

# Trigger ETF data fetch in the background
_ = fetch_all_etf_data()

# Display all assets with gap up filter
for symbol in asset_symbols:
    asset_name = TICKER_TO_NAME.get(symbol, symbol)
    df = fetch_rvol_data(symbol)
    # Gap up detection
    is_gap, curr_open_rvol, prev_open_rvol = detect_gap_up(df, open_hours, gap_threshold)
    if not is_gap:
        continue  # Skip assets that do not meet the gap up threshold
    st.subheader(f"{asset_name} ({symbol})")
    st.caption(f"Gap up detected: Current open rvol = {curr_open_rvol:.2f}, Previous open rvol = {prev_open_rvol:.2f}, Ratio = {curr_open_rvol/prev_open_rvol:.2f}")
    if df.empty:
        st.warning(f"No data found for {asset_name} ({symbol}).")
    else:
        # Convert datetime_gmt3 back to datetime for filtering
        df["datetime_gmt3_dt"] = pd.to_datetime(df["datetime_gmt3"], errors="coerce")
        df = df.dropna(subset=["datetime_gmt3_dt"])
        df = df.sort_values("datetime_gmt3_dt", ascending=False)
        df["date_gmt3"] = df["datetime_gmt3_dt"].dt.date
        df["hour_gmt3"] = df["datetime_gmt3_dt"].dt.hour
        # Isolate the latest available date (even if partial)
        if df.empty:
            st.warning(f"No valid datetime data for {asset_name} ({symbol}).")
        else:
            latest_day = df.iloc[0]["date_gmt3"]
            day_df = df[df["date_gmt3"] == latest_day].copy()
            # Only keep hours between 0 and 23
            day_df = day_df[(day_df["hour_gmt3"] >= 0) & (day_df["hour_gmt3"] <= 23)]
            if day_df.empty:
                st.warning(f"No data for {asset_name} ({symbol}) on latest day (hours 0-23).")
            else:
                # 70th percentile line from 2 years of rvol
                percentile_70 = df["rvol"].quantile(0.7)
                # Plot the latest day (partial or full)
                chart_df = day_df.set_index("hour_gmt3")[["rvol"]].sort_index()
                import plotly.graph_objs as go
                fig = go.Figure()
                fig.add_trace(go.Bar(x=chart_df.index, y=chart_df["rvol"], name="RVol", marker_color="blue"))
                fig.add_hline(y=percentile_70, line_width=3, line_dash="dash", line_color="red", annotation_text="70th percentile", annotation_position="top right")
                fig.update_layout(
                    title=f"{asset_name} ({symbol}) — {latest_day}",
                    xaxis_title="Hour of Day (GMT+3)",
                    yaxis_title="RVol",
                    xaxis=dict(tickmode='array', tickvals=list(range(24)), ticktext=[str(h) for h in range(24)]),
                    yaxis=dict(rangemode="tozero"),
                    height=300
                )
                st.plotly_chart(fig, use_container_width=True, key=f"rvol-{symbol}")

                # --- Sector Score Chart ---
                # Find the ETF symbol for this asset
                etf_info = ETF_MAP.get(symbol)
                sector = ASSET_TO_SECTOR.get(symbol)
                if not sector:
                    st.warning(f"No sector found for {asset_name} ({symbol}) in asset_category_map.json.")
                elif not etf_info:
                    st.warning(f"No ETF mapping found for {asset_name} ({symbol}), cannot compute sector score.")
                else:
                    etf_symbol = etf_info[0]
                    etf_df = fetch_rvol_data(etf_symbol)
                    if etf_df is None or etf_df.empty:
                        st.warning(f"No ETF data found for {etf_symbol} (asset ETF for {asset_name} ({symbol})).")
                    else:
                        # Prepare ETF data
                        etf_df = etf_df.copy()
                        etf_df["datetime_gmt3_dt"] = pd.to_datetime(etf_df["datetime_gmt3"], errors="coerce")
                        etf_df = etf_df.dropna(subset=["datetime_gmt3_dt"])
                        etf_df = etf_df.sort_values("datetime_gmt3_dt", ascending=False)
                        etf_df["date_gmt3"] = etf_df["datetime_gmt3_dt"].dt.date
                        etf_df["hour_gmt3"] = etf_df["datetime_gmt3_dt"].dt.hour
                        # Isolate ETF data for the latest day
                        if etf_df.empty:
                            st.warning(f"No valid datetime data for ETF {etf_symbol}.")
                        else:
                            etf_latest_day = etf_df.iloc[0]["date_gmt3"]
                            etf_day_df = etf_df[etf_df["date_gmt3"] == etf_latest_day].copy()
                            etf_hours = sorted(etf_day_df["hour_gmt3"].unique().tolist()) if not etf_day_df.empty else []
                            # Get previous day's 22:00 rvol for pre-market forward fill
                            prev_day = etf_latest_day - pd.Timedelta(days=1)
                            prev_day_df = etf_df[etf_df["date_gmt3"] == prev_day]
                            prev_22_rvol = None
                            if not prev_day_df.empty and 22 in prev_day_df["hour_gmt3"].tolist():
                                prev_22_rvol = prev_day_df[prev_day_df["hour_gmt3"] == 22]["rvol"].iloc[0]
                            # Forward fill ETF rvol for hours 0-23
                            etf_ffill = pd.DataFrame({"hour_gmt3": list(range(24))})
                            etf_ffill = etf_ffill.merge(etf_day_df[["hour_gmt3", "rvol"]], on="hour_gmt3", how="left")
                            # Fill 0-15 with previous day's 22:00 rvol
                            if prev_22_rvol is not None:
                                etf_ffill.loc[etf_ffill["hour_gmt3"] < 16, "rvol"] = prev_22_rvol
                            # Forward fill 16-22 with actual values, and 23 with 22:00 value
                            last_rvol = None
                            for idx, row in etf_ffill.iterrows():
                                h = row["hour_gmt3"]
                                if 16 <= h <= 22 and not pd.isna(row["rvol"]):
                                    last_rvol = row["rvol"]
                                elif h > 22:
                                    etf_ffill.at[idx, "rvol"] = last_rvol
                            # --- Calculate mean sector rvol for each hour ---
                            sector_assets = ASSET_CATEGORY_MAP[sector]
                            sector_rvols = []
                            for asset in sector_assets:
                                asset_df = fetch_rvol_data(asset)
                                asset_disp_name = TICKER_TO_NAME.get(asset, asset)
                                if asset_df is None or asset_df.empty:
                                    continue
                                asset_df = asset_df.copy()
                                asset_df["datetime_gmt3_dt"] = pd.to_datetime(asset_df["datetime_gmt3"], errors="coerce")
                                asset_df = asset_df.dropna(subset=["datetime_gmt3_dt"])
                                asset_df = asset_df.sort_values("datetime_gmt3_dt", ascending=False)
                                asset_df["date_gmt3"] = asset_df["datetime_gmt3_dt"].dt.date
                                asset_df["hour_gmt3"] = asset_df["datetime_gmt3_dt"].dt.hour
                                asset_day_df = asset_df[asset_df["date_gmt3"] == latest_day].copy()
                                asset_day_df = asset_day_df[(asset_day_df["hour_gmt3"] >= 0) & (asset_day_df["hour_gmt3"] <= 23)]
                                if not asset_day_df.empty:
                                    sector_rvols.append(asset_day_df.set_index("hour_gmt3")["rvol"])
                            if not sector_rvols:
                                st.warning(f"No sector rvol data available for sector {sector} on {latest_day}.")
                            else:
                                sector_rvol_mean = pd.concat(sector_rvols, axis=1).mean(axis=1)
                                # Merge mean sector rvol and ETF rvol on hour_gmt3 for the latest day
                                merged = pd.merge(
                                    sector_rvol_mean.rename("sector_rvol").reset_index(),
                                    etf_ffill[["hour_gmt3", "rvol"]].rename(columns={"rvol": "rvol_etf"}),
                                    on="hour_gmt3",
                                    how="inner"
                                )
                                if merged.empty:
                                    st.warning(f"No overlapping hourly data for sector {sector} and ETF {etf_symbol} on {latest_day}.")
                                else:
                                    # Calculate sector score for the latest day
                                    merged["sector_score"] = 0.4 * merged["rvol_etf"] + 0.6 * merged["sector_rvol"]
                                    # --- Calculate 82nd percentile from 2-year sector score data ---
                                    # Build 2-year sector score series (all hours, all assets in sector, and ETF)
                                    sector_rvols_2y = []
                                    for asset in sector_assets:
                                        asset_df_2y = fetch_rvol_data(asset)
                                        if asset_df_2y is None or asset_df_2y.empty:
                                            continue
                                        asset_df_2y = asset_df_2y.copy()
                                        asset_df_2y["datetime_gmt3_dt"] = pd.to_datetime(asset_df_2y["datetime_gmt3"], errors="coerce")
                                        asset_df_2y = asset_df_2y.dropna(subset=["datetime_gmt3_dt"])
                                        asset_df_2y = asset_df_2y.sort_values("datetime_gmt3_dt")
                                        asset_df_2y["hour_gmt3"] = asset_df_2y["datetime_gmt3_dt"].dt.hour
                                        sector_rvols_2y.append(asset_df_2y["rvol"])
                                    if sector_rvols_2y:
                                        sector_rvols_2y_all = pd.concat(sector_rvols_2y, axis=0)
                                    else:
                                        sector_rvols_2y_all = pd.Series(dtype=float)
                                    etf_df_2y = fetch_rvol_data(etf_symbol)
                                    if etf_df_2y is not None and not etf_df_2y.empty:
                                        etf_df_2y = etf_df_2y.copy()
                                        etf_df_2y["datetime_gmt3_dt"] = pd.to_datetime(etf_df_2y["datetime_gmt3"], errors="coerce")
                                        etf_df_2y = etf_df_2y.dropna(subset=["datetime_gmt3_dt"])
                                        etf_df_2y = etf_df_2y.sort_values("datetime_gmt3_dt")
                                        etf_rvol_2y = etf_df_2y["rvol"]
                                    else:
                                        etf_rvol_2y = pd.Series(dtype=float)
                                    # Calculate sector score for all available hours in 2 years
                                    sector_score_2y = []
                                    if not sector_rvols_2y_all.empty and not etf_rvol_2y.empty:
                                        # Align lengths by truncating to the shortest
                                        min_len = min(len(sector_rvols_2y_all), len(etf_rvol_2y))
                                        sector_score_2y = 0.4 * etf_rvol_2y.iloc[:min_len].values + 0.6 * sector_rvols_2y_all.iloc[:min_len].values
                                        sector_score_2y = pd.Series(sector_score_2y)
                                    elif not sector_rvols_2y_all.empty:
                                        sector_score_2y = sector_rvols_2y_all
                                    elif not etf_rvol_2y.empty:
                                        sector_score_2y = etf_rvol_2y
                                    else:
                                        sector_score_2y = pd.Series(dtype=float)
                                    percentile_82 = sector_score_2y.quantile(0.82) if not sector_score_2y.empty else None
                                    # Plot sector score for the latest day
                                    sector_chart_df = merged.set_index("hour_gmt3")[["sector_score"]].sort_index()
                                    fig2 = go.Figure()
                                    fig2.add_trace(go.Bar(x=sector_chart_df.index, y=sector_chart_df["sector_score"], name="Sector Score", marker_color="orange"))
                                    if percentile_82 is not None:
                                        fig2.add_hline(y=percentile_82, line_width=3, line_dash="dash", line_color="purple", annotation_text="82nd percentile (2y)", annotation_position="top right")
                                    fig2.update_layout(
                                        title=f"Sector Score — {sector} ({etf_symbol}) — {latest_day}",
                                        xaxis_title="Hour of Day (GMT+3)",
                                        yaxis_title="Sector Score",
                                        xaxis=dict(tickmode='array', tickvals=list(range(24)), ticktext=[str(h) for h in range(24)]),
                                        yaxis=dict(rangemode="tozero"),
                                        height=300
                                    )
                                    st.plotly_chart(fig2, use_container_width=True, key=f"sector-{symbol}")
    st.markdown('---') 