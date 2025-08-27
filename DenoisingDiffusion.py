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
COINGECKO_API_KEY = ""  # leave blank for free API
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
    # liquidity: volume scaled by inverse of volatility (proxy)
    out["liquidity"] = (out["norm_volume"] / (out["volatility"] + 1e-8)).fillna(method="bfill").fillna(0.0)
    # imbalance: signed flow proxy (positive => demand > supply)
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
    default_lowcaps = ["akash", "celestia", "injective-protocol", "beam", "sei-network",
                       "sui", "near", "render-token", "optimism", "arbitrum"]
    coin_id = st.text_input("CoinGecko coin id", value=default_lowcaps[0])

# Validate ID when using CoinGecko
if source == "CoinGecko (auto)":
    try:
        valid_ids = fetch_coingecko_ids()
        if coin_id not in valid_ids:
            st.warning(f"Coin id '{coin_id}' not found on CoinGecko. Try one of: {', '.join(default_lowcaps)}")
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
                raw = df.rename(columns={c: c for c in df.columns})
                df = derive_liquidity_and_imbalance(raw)
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
alpha_L = st.sidebar.number_input("alpha_L (liq inflow)", value=0.05, step=0.01, format="%.3f")
beta_L = st.sidebar.number_input("beta_L (liq withdrawal)", value=0.10, step=0.01, format="%.3f")
rho_I = st.sidebar.number_input("rho_I (imb persistence)", value=0.95, step=0.01, format="%.3f")
mu_I = st.sidebar.number_input("mu_I (ret sensitivity to imbalance)", value=-0.02, step=0.01, format="%.3f")
sigma0 = st.sidebar.number_input("sigma0 (base vol)", value=0.03, step=0.005, format="%.4f")
kappa = st.sidebar.number_input("kappa (liq→vol scale)", value=0.5, step=0.05, format="%.3f")
gamma = st.sidebar.number_input("gamma (liq exponent)", value=0.5, step=0.05, format="%.3f")
crash_pct = st.sidebar.number_input("Crash threshold (price drop %)", value=0.25, min_value=0.01, max_value=0.9, step=0.01, format="%.2f")
L_min_ratio = st.sidebar.number_input("Liquidity exhaustion ratio", value=0.10, min_value=0.01, max_value=1.0, step=0.01, format="%.2f")
I_crit = st.sidebar.number_input("Imbalance critical (neg)", value=0.20, min_value=0.01, max_value=2.0, step=0.01, format="%.3f")
persist_days = st.sidebar.number_input("Liquidity persistence days", value=3, min_value=1, max_value=30, step=1)

# ---------------------------
# Monte Carlo forward simulation
# ---------------------------
def simulate_forward(df_last: pd.DataFrame, H: int, N: int, alpha_L: float, beta_L: float,
                     rho_I: float, mu_I: float, sigma0: float, kappa: float, gamma: float) -> np.ndarray:
    """
    Returns simulated future price paths (N x H)
    """
    L0 = df_last["liquidity"].iloc[-1]
    I0 = df_last["imbalance"].iloc[-1]
    P0 = df_last["price"].iloc[-1]

    sim_prices = np.zeros((N, H))
    for n in range(N):
        L = L0
        I = I0
        P = P0
        path = []
        for t in range(H):
            # liquidity dynamics
            dL = np.random.normal(0, 0.01) - alpha_L*L + beta_L*(L0 - L)
            L = max(L + dL, 1e-8)
            # imbalance dynamics
            I = rho_I * I + np.random.normal(0, 0.01)
            # vol as function of liquidity
            sigma_t = sigma0 + kappa * (1/L)**gamma
            dP = mu_I*I + np.random.normal(0, sigma_t)
            P = max(P*(1 + dP), 1e-8)
            path.append(P)
        sim_prices[n, :] = path
    return sim_prices

sim_matrix = simulate_forward(df, H, N_MC, alpha_L, beta_L, rho_I, mu_I, sigma0, kappa, gamma)

# ---------------------------
# Compute ECDS (Early Crash Detection Score)
# ---------------------------
def compute_ECDS(sim_matrix: np.ndarray, crash_pct: float = 0.25) -> float:
    """
    Probability that simulated paths drop more than crash_pct
    """
    drops = (sim_matrix[:, -1] / sim_matrix[:, 0]) - 1.0
    prob_crash = np.mean(drops <= -crash_pct)
    return prob_crash

ECDS = compute_ECDS(sim_matrix, crash_pct)
st.metric("Early Crash Detection Score (ECDS)", f"{ECDS:.2%}")

# ---------------------------
# Build long/short strategies
# ---------------------------
def build_strategies(df: pd.DataFrame, ECDS: float):
    long_s1 = (ECDS < 0.3).astype(float)
    long_s2 = (ECDS < 0.2).astype(float)
    long_s3 = (df["liquidity"].pct_change().fillna(0) > 0).astype(float)

    short_a = (ECDS > 0.3).astype(float)
    short_b = ((df["imbalance"] < -I_crit) & (df["liquidity"] < L_min_ratio * df["liquidity"].rolling(persist_days, min_periods=1).mean())).astype(float)
    short_c = ((df["ret"] < -0.03).astype(float))
    return pd.DataFrame({
        "long_s1": long_s1,
        "long_s2": long_s2,
        "long_s3": long_s3,
        "short_a": short_a,
        "short_b": short_b,
        "short_c": short_c
    }, index=df.index)

strategies = build_strategies(df, ECDS)
st.subheader("Strategy Signals")
st.dataframe(strategies.tail(5), use_container_width=True)

# ---------------------------
# Backtest PnL Engine
# ---------------------------
def run_backtest(df: pd.DataFrame, strategies: pd.DataFrame, start_balance: float = 600.0) -> pd.DataFrame:
    eq_curves = pd.DataFrame(index=df.index)
    for strat in strategies.columns:
        pos = strategies[strat]
        price = df["price"]
        pnl = start_balance * (pos.shift(1).fillna(0) * price.pct_change().fillna(0) + 1.0).cumprod()
        eq_curves[strat] = pnl
    return eq_curves

equity_curves = run_backtest(df, strategies, START_BALANCE)

# ---------------------------
# Decision log
# ---------------------------
def build_decision_log(df: pd.DataFrame, eq_curves: pd.DataFrame) -> pd.DataFrame:
    log = []
    for strat in eq_curves.columns:
        pos = strategies[strat]
        prev = 0
        for t in range(len(df)):
            curr = pos.iloc[t]
            if curr != prev:
                log.append({
                    "date": df["date"].iloc[t],
                    "strategy": strat,
                    "signal": "ENTER" if curr > 0 else "EXIT",
                    "equity": eq_curves[strat].iloc[t]
                })
            prev = curr
    return pd.DataFrame(log)

decision_log = build_decision_log(df, equity_curves)

# ---------------------------
# Performance metrics
# ---------------------------
def compute_metrics(eq_curves: pd.DataFrame) -> pd.DataFrame:
    metrics = {}
    for strat in eq_curves.columns:
        equity = eq_curves[strat]
        ret = equity.pct_change().fillna(0)
        cumret = equity.iloc[-1] / equity.iloc[0] - 1
        max_dd = (equity / equity.cummax() - 1.0).min()
        sharpe = np.mean(ret) / (np.std(ret) + 1e-8) * np.sqrt(252)
        metrics[strat] = {
            "Final Balance": equity.iloc[-1],
            "Total Return %": cumret*100,
            "Max Drawdown %": max_dd*100,
            "Sharpe": sharpe
        }
    return pd.DataFrame(metrics).T

metrics_df = compute_metrics(equity_curves)

# ---------------------------
# Streamlit Display
# ---------------------------
st.subheader("Equity Curves")
st.line_chart(equity_curves, use_container_width=True)

st.subheader("Decision Log")
st.dataframe(decision_log, use_container_width=True)

st.subheader("Performance Metrics")
st.dataframe(metrics_df.style.format("{:.2f}"), use_container_width=True)

st.markdown("**Disclaimer:** This tool is for educational and research purposes. Not financial advice.")
