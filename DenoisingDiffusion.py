# app.py
# Low-Cap Crypto Crash Simulator & Backtest with Early Short-Entry Strategies
# Single-file Streamlit app: data -> ECDS -> Strategies -> PnL & performance
# WARNING: This is a research/backtest tool. Not financial advice.

import streamlit as st
import pandas as pd
import numpy as np
import requests
from typing import Dict, List
from scipy.stats import norm

# ---------------------------
# Configuration
# ---------------------------
COINGECKO_API_KEY = "CG-chRgqiH9ab4zsFTm2Zvst82a"  # user-provided; keep private
HEADERS: Dict[str, str] = {"x-cg-pro-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

START_BALANCE = 600.0  # starting capital per strategy

st.set_page_config(page_title="Low-Cap Crypto Crash Backtest", layout="wide")
st.title("Low-Cap Crypto Sector Crash Simulator & Backtest (Long + Short)")

st.markdown("""
**What this app does**

- Downloads historical daily data from CoinGecko (or lets you upload CSV).
- Computes liquidity & supply/demand imbalance proxies.
- Runs a hybrid Monte Carlo forward simulator (liquidity + imbalance dynamics).
- Builds an Early Crash Detection Score (ECDS) and three long/three short strategies.
- Backtests strategies with **$600 starting capital per strategy**; shows equity curves, decision log, and performance.
""")

# ---------------------------
# Utility: validate CoinGecko IDs (cached)
# ---------------------------
@st.cache_data(ttl=60*60)
def fetch_coingecko_ids() -> List[str]:
    url = f"{COINGECKO_BASE}/coins/list"
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    coins = r.json()
    return [c["id"] for c in coins]

# ---------------------------
# Small-cap coin filtering
# ---------------------------
@st.cache_data(ttl=60*60)
def fetch_smallcap_coins(vs_currency: str = "usd", market_cap_max: float = 500_000_000) -> List[str]:
    """
    Fetch coins with market cap below `market_cap_max`.
    """
    url = f"{COINGECKO_BASE}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_asc",
        "per_page": 250,
        "page": 1,
        "price_change_percentage": "1h"
    }
    smallcaps = []
    while True:
        r = requests.get(url, params=params, headers=HEADERS, timeout=30)
        r.raise_for_status()
        data = r.json()
        if not data:
            break
        for coin in data:
            if coin.get("market_cap", 0) <= market_cap_max:
                smallcaps.append(coin["id"])
        # Stop if last page or first coin above threshold
        if len(data) < 250 or any(coin.get("market_cap", 0) > market_cap_max for coin in data):
            break
        params["page"] += 1
    return smallcaps

# ---------------------------
# Data loader from CoinGecko
# ---------------------------
@st.cache_data(show_spinner=False, ttl=60*30)
def cg_coin_market_chart(coin_id: str, vs_currency: str, days: int) -> pd.DataFrame:
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    if r.status_code != 200:
        raise ValueError(f"CoinGecko API error ({r.status_code}): {r.text}")
    data = r.json()

    def to_df(lst, col):
        df = pd.DataFrame(lst, columns=["ts_ms", col])
        df["date"] = pd.to_datetime(df["ts_ms"], unit="ms").dt.tz_localize(None)
        return df[["date", col]]

    prices = to_df(data.get("prices", []), "price")
    volumes = to_df(data.get("total_volumes", []), "volume")
    mktcap = to_df(data.get("market_caps", []), "market_cap")
    df = prices.merge(volumes, on="date", how="outer").merge(mktcap, on="date", how="outer")
    df.sort_values("date", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df

# ---------------------------
# Feature engineering: liquidity & imbalance proxies
# ---------------------------
def derive_liquidity_and_imbalance(df: pd.DataFrame, vol_window: int = 14, flow_window: int = 7) -> pd.DataFrame:
    out = df.copy()
    out["ret"] = np.log(out["price"]).diff()
    out["volatility"] = out["ret"].rolling(vol_window, min_periods=1).std()
    out["norm_volume"] = out["volume"].rolling(vol_window, min_periods=1).mean()
    out["liquidity"] = (out["norm_volume"] / (out["volatility"] + 1e-8)).fillna(method="bfill").fillna(0.0)
    out["imbalance"] = (out["ret"].rolling(flow_window, min_periods=1).mean() -
                        out["ret"].rolling(flow_window, min_periods=1).median()).fillna(0.0)
    out = out.rename(columns={"market_cap": "market_cap"})
    return out[["date", "price", "liquidity", "imbalance", "ret", "volume", "market_cap"]]

# ---------------------------
# Sidebar: data / coin selection
# ---------------------------
st.sidebar.header("Data Source")
source = st.sidebar.selectbox("Source", ["CoinGecko (auto)", "Upload CSV"])
uploaded_df = None
if source == "Upload CSV":
    uploaded = st.sidebar.file_uploader("Upload CSV (columns: date, price, liquidity, imbalance, volume)", type="csv")
    if uploaded:
        uploaded_df = pd.read_csv(uploaded, parse_dates=["date"]).sort_values("date")

with st.sidebar.expander("Coin selection", expanded=True):
    vs_currency = st.selectbox("Quote currency", ["usd", "eur", "btc"], index=0)
    days_back = st.number_input("History (days)", min_value=60, max_value=3650, value=365, step=30)
    # small-cap list
    smallcap_list = fetch_smallcap_coins(vs_currency=vs_currency)
    if smallcap_list:
        coin_id = st.selectbox("Pick a small-cap coin", options=smallcap_list, index=0)
    else:
        default_lowcaps = ["akash", "celestia", "injective-protocol", "beam", "sei-network",
                           "sui", "near", "render-token", "optimism", "arbitrum"]
        st.warning("Could not fetch small-cap coins; fallback to default list.")
        coin_id = st.selectbox("Pick a coin", options=default_lowcaps, index=0)

# Validate ID
if source == "CoinGecko (auto)":
    try:
        valid_ids = fetch_coingecko_ids()
        if coin_id not in valid_ids:
            st.warning(f"Coin id '{coin_id}' not found on CoinGecko.")
    except Exception as e:
        st.warning(f"Could not fetch coin list for validation: {e}")
        valid_ids = None

# Load data
df = None
if source == "CoinGecko (auto)":
    try:
        raw = cg_coin_market_chart(coin_id, vs_currency, int(days_back))
        df = derive_liquidity_and_imbalance(raw)
        st.success(f"Loaded {len(df)} rows for '{coin_id}'.")
        st.dataframe(df.tail(5), use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load data from CoinGecko: {e}")
        df = None
else:
    df = uploaded_df
    if df is not None:
        if not {"price", "liquidity", "imbalance"}.issubset(set(df.columns)):
            try:
                df = derive_liquidity_and_imbalance(df)
            except Exception as e:
                st.error(f"Uploaded CSV missing expected columns and auto-derivation failed: {e}")
                st.stop()
        st.success(f"Loaded uploaded CSV with {len(df)} rows.")
        st.dataframe(df.tail(5), use_container_width=True)

if df is None or len(df) < 60:
    st.stop()

# ---------------------------
# Simulation parameters (Sidebar)
# ---------------------------
st.sidebar.header("Simulation & Model Parameters")
H = st.sidebar.number_input("Simulation horizon (days)", value=10, min_value=2, max_value=365)
N_MC = st.sidebar.number_input("Monte Carlo paths", value=300, min_value=50, max_value=2000, step=50)
alpha_L = st.sidebar.number_input("alpha_L (long aggressiveness)", value=0.25, step=0.05, min_value=0.0, max_value=1.0)
alpha_S = st.sidebar.number_input("alpha_S (short aggressiveness)", value=0.25, step=0.05, min_value=0.0, max_value=1.0)
lookback_window = st.sidebar.number_input("ECDS lookback window", value=14, min_value=2, max_value=60)

# ---------------------------
# Monte Carlo simulation
# ---------------------------
def mc_simulate(df, H, N_MC):
    last_price = df["price"].iloc[-1]
    mu = df["ret"].mean()
    sigma = df["ret"].std()
    sim_matrix = np.zeros((H, N_MC))
    for i in range(N_MC):
        ret_path = np.random.normal(mu, sigma, H)
        sim_matrix[:, i] = last_price * np.exp(np.cumsum(ret_path))
    sim_df = pd.DataFrame(sim_matrix)
    return sim_df

st.subheader("Monte Carlo Price Paths")
sim_df = mc_simulate(df, H, N_MC)
st.line_chart(sim_df)

# ---------------------------
# Early Crash Detection Score (ECDS)
# ---------------------------
def ecds(df, lookback=14):
    score = df["liquidity"].rolling(lookback).mean() / (df["imbalance"].rolling(lookback).std() + 1e-8)
    return (score - score.min()) / (score.max() - score.min() + 1e-8)

df["ECDS"] = ecds(df, lookback_window)
st.subheader("Early Crash Detection Score (ECDS)")
st.line_chart(df[["ECDS"]])

# ---------------------------
# Simple long/short strategies
# ---------------------------
def backtest_strategies(df, start_balance=START_BALANCE):
    results = {}
    for sname, rule in {
        "Long-Low-Liquidity": lambda x: x["liquidity"].iloc[-1] < x["liquidity"].rolling(lookback_window).mean().iloc[-1],
        "Long-High-Imbalance": lambda x: x["imbalance"].iloc[-1] > x["imbalance"].rolling(lookback_window).mean().iloc[-1],
        "Long-ECDS": lambda x: x["ECDS"].iloc[-1] < 0.5,
        "Short-High-Liquidity": lambda x: x["liquidity"].iloc[-1] > x["liquidity"].rolling(lookback_window).mean().iloc[-1],
        "Short-Low-Imbalance": lambda x: x["imbalance"].iloc[-1] < x["imbalance"].rolling(lookback_window).mean().iloc[-1],
        "Short-ECDS": lambda x: x["ECDS"].iloc[-1] > 0.5
    }.items():
        balance = start_balance
        pnl = []
        for i in range(1, len(df)):
            entry = rule(df.iloc[:i+1])
            ret = df["ret"].iloc[i]
            balance *= 1 + (ret if "Long" in sname else -ret) * (alpha_L if "Long" in sname else alpha_S)
            pnl.append(balance)
        results[sname] = pnl
    return pd.DataFrame(results)

st.subheader("Backtest Strategies (Equity Curves)")
pnl_df = backtest_strategies(df)
st.line_chart(pnl_df)
st.dataframe(pnl_df.tail(5), use_container_width=True)

# ---------------------------
# Download results CSV
# ---------------------------
csv = pnl_df.to_csv(index=False).encode()
st.download_button("Download Backtest CSV", csv, f"{coin_id}_backtest.csv", "text/csv")

# ---------------------------
# End of script
# ---------------------------
st.info("Simulation complete. Adjust parameters or coin selection to re-run.")
