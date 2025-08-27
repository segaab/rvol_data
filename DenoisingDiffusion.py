# app.py
# Low-Cap Crypto Crash Simulator & Backtest with Early Short-Entry Strategies
# Streamlit app with robust API error handling

import streamlit as st
import pandas as pd
import numpy as np
import requests
from typing import List, Dict
from scipy.stats import norm

# ---------------------------
# Configuration
# ---------------------------
COINGECKO_API_KEY = ""  # Optional API key
HEADERS: Dict[str, str] = {"x-cg-pro-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

START_BALANCE = 600.0

st.set_page_config(page_title="Low-Cap Crypto Crash Backtest", layout="wide")
st.title("Low-Cap Crypto Sector Crash Simulator & Backtest (Long + Short)")

st.markdown("""
**This app:**
- Fetches crypto data (or upload CSV)
- Calculates liquidity & imbalance proxies
- Runs Monte Carlo simulation
- Computes Early Crash Detection Score (ECDS)
- Backtests long/short strategies with $600 start per strategy
""")

# ---------------------------
# CoinGecko Utilities
# ---------------------------
@st.cache_data(ttl=60*60)
def fetch_coingecko_ids() -> List[str]:
    try:
        url = f"{COINGECKO_BASE}/coins/list"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        coins = r.json()
        return [c["id"] for c in coins]
    except Exception:
        return []

@st.cache_data(ttl=60*60)
def fetch_smallcap_coins(vs_currency: str = "usd", market_cap_max: float = 500_000_000) -> List[str]:
    """
    Fetch coins with market cap below `market_cap_max`.
    Fallback to default list if API fails.
    """
    default_smallcaps = [
        "akash", "celestia", "injective-protocol", "beam", "sei-network",
        "sui", "near", "render-token", "optimism", "arbitrum"
    ]
    try:
        url = f"{COINGECKO_BASE}/coins/markets"
        params = {"vs_currency": vs_currency, "order": "market_cap_asc",
                  "per_page": 250, "page": 1}
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
            if len(data) < 250 or any(coin.get("market_cap", 0) > market_cap_max for coin in data):
                break
            params["page"] += 1
        if not smallcaps:
            return default_smallcaps
        return smallcaps
    except Exception:
        return default_smallcaps

@st.cache_data(show_spinner=False, ttl=60*30)
def cg_coin_market_chart(coin_id: str, vs_currency: str, days: int) -> pd.DataFrame:
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
    r = requests.get(url, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
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
# Feature engineering
# ---------------------------
def derive_liquidity_and_imbalance(df: pd.DataFrame, vol_window: int = 14, flow_window: int = 7) -> pd.DataFrame:
    out = df.copy()
    out["ret"] = np.log(out["price"]).diff()
    out["volatility"] = out["ret"].rolling(vol_window, min_periods=1).std()
    out["norm_volume"] = out["volume"].rolling(vol_window, min_periods=1).mean()
    out["liquidity"] = (out["norm_volume"] / (out["volatility"] + 1e-8)).fillna(method="bfill").fillna(0.0)
    out["imbalance"] = (out["ret"].rolling(flow_window, min_periods=1).mean() -
                        out["ret"].rolling(flow_window, min_periods=1).median()).fillna(0.0)
    return out[["date", "price", "liquidity", "imbalance", "ret", "volume", "market_cap"]]

# ---------------------------
# Sidebar
# ---------------------------
st.sidebar.header("Data Source")
source = st.sidebar.selectbox("Source", ["CoinGecko (auto)", "Upload CSV"])
uploaded_df = None
if source == "Upload CSV":
    uploaded = st.sidebar.file_uploader("Upload CSV", type="csv")
    if uploaded:
        uploaded_df = pd.read_csv(uploaded, parse_dates=["date"]).sort_values("date")

st.sidebar.header("Coin Selection")
vs_currency = st.selectbox("Quote currency", ["usd", "eur", "btc"], index=0)
days_back = st.number_input("History (days)", min_value=60, max_value=3650, value=365, step=30)
smallcap_list = fetch_smallcap_coins(vs_currency=vs_currency)
coin_id = st.selectbox("Pick a coin", options=smallcap_list, index=0)

# ---------------------------
# Load data
# ---------------------------
df = None
if source == "CoinGecko (auto)":
    try:
        raw = cg_coin_market_chart(coin_id, vs_currency, int(days_back))
        df = derive_liquidity_and_imbalance(raw)
        st.success(f"Loaded {len(df)} rows for '{coin_id}'.")
        st.dataframe(df.tail(5), use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load CoinGecko data: {e}")
        st.stop()
else:
    df = uploaded_df
    if df is not None:
        if not {"price", "liquidity", "imbalance"}.issubset(df.columns):
            df = derive_liquidity_and_imbalance(df)

if df is None or len(df) < 60:
    st.stop()

# ---------------------------
# Sidebar: simulation
# ---------------------------
st.sidebar.header("Simulation")
H = st.sidebar.number_input("Horizon (days)", value=10, min_value=2, max_value=365)
N_MC = st.sidebar.number_input("Monte Carlo paths", value=300, min_value=50, max_value=2000)
alpha_L = st.sidebar.number_input("Long aggressiveness", value=0.25, min_value=0.0, max_value=1.0)
alpha_S = st.sidebar.number_input("Short aggressiveness", value=0.25, min_value=0.0, max_value=1.0)
lookback_window = st.sidebar.number_input("ECDS lookback", value=14, min_value=2, max_value=60)

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
    return pd.DataFrame(sim_matrix)

st.subheader("Monte Carlo Price Paths")
sim_df = mc_simulate(df, H, N_MC)
st.line_chart(sim_df)

# ---------------------------
# ECDS
# ---------------------------
def ecds(df, lookback=14):
    score = df["liquidity"].rolling(lookback).mean() / (df["imbalance"].rolling(lookback).std() + 1e-8)
    return (score - score.min()) / (score.max() - score.min() + 1e-8)

df["ECDS"] = ecds(df, lookback_window)
st.subheader("Early Crash Detection Score")
st.line_chart(df[["ECDS"]])

# ---------------------------
# Strategies
# ---------------------------
def backtest_strategies(df, start_balance=START_BALANCE):
    results = {}
    for sname, rule in {
        "Long-Low-Liq": lambda x: x["liquidity"].iloc[-1] < x["liquidity"].rolling(lookback_window).mean().iloc[-1],
        "Long-High-Imb": lambda x: x["imbalance"].iloc[-1] > x["imbalance"].rolling(lookback_window).mean().iloc[-1],
        "Long-ECDS": lambda x: x["ECDS"].iloc[-1] < 0.5,
        "Short-High-Liq": lambda x: x["liquidity"].iloc[-1] > x["liquidity"].rolling(lookback_window).mean().iloc[-1],
        "Short-Low-Imb": lambda x: x["imbalance"].iloc[-1] < x["imbalance"].rolling(lookback_window).mean().iloc[-1],
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

st.subheader("Backtest Equity Curves")
pnl_df = backtest_strategies(df)
st.line_chart(pnl_df)
st.dataframe(pnl_df.tail(5), use_container_width=True)

# ---------------------------
# CSV download
# ---------------------------
csv = pnl_df.to_csv(index=False).encode()
st.download_button("Download CSV", csv, f"{coin_id}_backtest.csv", "text/csv")

st.info("Simulation complete. Adjust parameters or coin selection to re-run.")
