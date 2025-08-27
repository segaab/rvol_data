# ---------------------------
# Chunk 1: Data loading, preprocessing, and Monte Carlo crash simulation
# ---------------------------

import streamlit as st
import pandas as pd
import numpy as np
import requests
from typing import Dict
from scipy.stats import norm

# ---------------------------
# Hard-coded CoinGecko Pro key
# ---------------------------
COINGECKO_API_KEY = "CG-chRgqiH9ab4zsFTm2Zvst82a"
HEADERS: Dict[str, str] = {"x-cg-pro-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

st.set_page_config(page_title="Low-Cap Crypto Crash Backtest (Long + Short)", layout="wide")
st.title("Low-Cap Crypto Sector Crash Simulator & Backtest (Long + Short)")

st.markdown("""
This dashboard simulates sector crash risk using liquidity and imbalance dynamics, and backtests:
- 3 long-side risk-mitigation strategies (reduce/exit when risk rises)
- 3 short-entry strategies with an early detection system

Data: CoinGecko API (Pro header included). Research tool only; not financial advice.
""")

# ---------------------------
# Data helpers
# ---------------------------
@st.cache_data(show_spinner=False, ttl=60*30)
def cg_coin_market_chart(coin_id: str, vs_currency: str, days: int):
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
    return df

def derive_liquidity_and_imbalance(df: pd.DataFrame, vol_window: int = 14, flow_window: int = 7) -> pd.DataFrame:
    out = df.copy()
    out["ret"] = np.log(out["price"]).diff()
    out["volatility"] = out["ret"].rolling(vol_window).std()
    out["norm_volume"] = out["volume"].rolling(vol_window).mean()
    # Liquidity proxy: higher when volume is higher and volatility is lower
    out["liquidity"] = (out["norm_volume"] / (out["volatility"] + 1e-8)).fillna(method="bfill").fillna(0)
    # Imbalance proxy: recent signed drift
    out["imbalance"] = (out["ret"].rolling(flow_window).mean() - out["ret"].rolling(flow_window).median()).fillna(0)
    return out[["date", "price", "liquidity", "imbalance", "ret", "volume", "market_cap"]]

# ---------------------------
# Sidebar: data selection
# ---------------------------
st.sidebar.header("Data")
source = st.sidebar.selectbox("Source", ["CoinGecko (auto)", "Upload CSV"])

uploaded_df = None
if source == "Upload CSV":
    uploaded = st.sidebar.file_uploader("CSV with columns: date, price, liquidity, imbalance", type="csv")
    if uploaded:
        uploaded_df = pd.read_csv(uploaded, parse_dates=["date"]).sort_values("date")

with st.sidebar.expander("Low-cap universe selection", expanded=True):
    vs_currency = st.selectbox("Quote currency", ["usd", "eur", "btc"], index=0)
    days_back = st.number_input("History (days)", min_value=30, max_value=1825, value=365, step=30)
    default_lowcaps = ["akash-network","celestia","injective-protocol","beam-2","sei-network","sui","near","render-token","optimism","arbitrum"]
    coin_id = st.text_input("CoinGecko coin id", value=default_lowcaps[0])

if source == "CoinGecko (auto)":
    try:
        raw = cg_coin_market_chart(coin_id, vs_currency, int(days_back))
        df = derive_liquidity_and_imbalance(raw)
        st.success(f"Loaded {len(df)} daily points for {coin_id}.")
        st.dataframe(df.tail(10), use_container_width=True)
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        df = None
else:
    df = uploaded_df
    if df is not None:
        st.success(f"Loaded uploaded data with {len(df)} rows.")
        st.dataframe(df.tail(10), use_container_width=True)

if df is None or len(df) < 60:
    st.stop()
# ---------------------------
# Chunk 2: PnL computation (long and short) with starting balance
# ---------------------------
def compute_pnl(long_df: pd.DataFrame, start_balance: float = 600.0, fee_bps: float = 5.0, borrow_bps_daily: float = 1.0):
    d = long_df.copy()
    d["ret"] = d["ret"].fillna(0.0)

    # --- Long strategies ---
    for col in ["position_s1", "position_s2", "position_s3"]:
        pos = d[col].fillna(0.0).values
        pos_shift = np.roll(pos, 1)
        pos_shift[0] = 0.0
        turnover = np.abs(pos - pos_shift)
        tc = turnover * (fee_bps / 1e4)
        # Apply starting balance
        d[f"{col}_pnl"] = start_balance * (pos * d["ret"] - tc)

    # --- Short strategies ---
    borrow_cost = borrow_bps_daily / 1e4
    for scol in ["short_A", "short_B", "short_C"]:
        pos = short_bt[scol].reindex(d.index).fillna(0.0).values
        pos_shift = np.roll(pos, 1)
        pos_shift[0] = 0.0
        turnover = np.abs(pos - pos_shift)
        tc = turnover * (fee_bps / 1e4)
        # Short PnL: -ret minus costs
        d[f"{scol}_pnl"] = start_balance * (pos * (-d["ret"]) - tc - pos * borrow_cost)

    # --- Benchmark: buy-and-hold ---
    d["bh_pnl"] = start_balance * d["ret"]

    # --- Equity curves ---
    eq = d.set_index("date")[[
        "position_s1_pnl", "position_s2_pnl", "position_s3_pnl",
        "short_A_pnl", "short_B_pnl", "short_C_pnl",
        "bh_pnl"
    ]].cumsum() + start_balance

    return d, eq

# Run PnL computation
bt_row, equity_curves = compute_pnl(long_bt)

# Streamlit display: equity curves
st.subheader("Equity curves (with $600 starting balance)")
st.line_chart(equity_curves, use_container_width=True)

# ---------------------------
# Chunk 3: Decision log, PnL computation, and performance summary
# ---------------------------

# ---------------------------
# PnL computation (long and short)
# ---------------------------
def compute_pnl(long_df: pd.DataFrame, short_df: pd.DataFrame, start_cap: float = 600.0, fee_bps: float = 5.0, borrow_bps_daily: float = 1.0):
    d = long_df.copy()
    d["ret"] = d["ret"].fillna(0.0)

    # Long strategies: PnL = start_cap * position * ret - transaction cost
    for col in ["position_s1", "position_s2", "position_s3"]:
        pos = d[col].fillna(0.0).values
        pos_shift = np.roll(pos, 1); pos_shift[0] = 0.0
        turnover = np.abs(pos - pos_shift)
        tc = turnover * (fee_bps / 1e4) * start_cap
        d[f"{col}_pnl"] = start_cap * pos * d["ret"] - tc

    # Short strategies: PnL = start_cap * position * (-ret) - transaction cost - borrow cost
    borrow_cost = borrow_bps_daily / 1e4 * start_cap
    for scol in ["short_A", "short_B", "short_C"]:
        pos = short_df[scol].reindex(d.index).fillna(0.0).values
        pos_shift = np.roll(pos, 1); pos_shift[0] = 0.0
        turnover = np.abs(pos - pos_shift)
        tc = turnover * (fee_bps / 1e4) * start_cap
        d[f"{scol}_pnl"] = start_cap * pos * (-d["ret"]) - tc - pos * borrow_cost

    # Benchmark: buy-and-hold long
    d["bh_pnl"] = start_cap * d["ret"]

    # Equity curves (cumulated)
    eq = d.set_index("date")[[
        "position_s1_pnl", "position_s2_pnl", "position_s3_pnl",
        "short_A_pnl", "short_B_pnl", "short_C_pnl",
        "bh_pnl"
    ]].cumsum()
    return d, eq

bt_row, equity_curves = compute_pnl(long_bt, short_bt, start_cap=600.0)
st.subheader("Equity curves (normalized)")
st.line_chart(equity_curves, use_container_width=True)

# ---------------------------
# Decision log
# ---------------------------
def build_decision_log(d: pd.DataFrame, H: int = 10, crash_pct: float = 0.25) -> pd.DataFrame:
    log = d[[
        "date","price","liquidity","imbalance","ret",
        "pred_crash_prob","ECDS","vol","liq_inv","prob_slope","neg_mom",
        "position_s1","position_s2","position_s3",
    ]].copy()
    log["short_A"] = short_bt["short_A"].reindex(log.index).values
    log["short_B"] = short_bt["short_B"].reindex(log.index).values
    log["short_C"] = short_bt["short_C"].reindex(log.index).values
    # Forward crash realization over H
    fwd = d["price"].shift(-int(H)) / d["price"] - 1.0
    log[f"fwd_return_{int(H)}d"] = fwd
    log[f"fwd_crash_{int(H)}d"] = (fwd <= -crash_pct).astype(int)
    return log

decision_log = build_decision_log(bt_row, H=int(H), crash_pct=crash_pct)
with st.expander("Decision log (exportable)"):
    st.dataframe(decision_log.tail(20), use_container_width=True)
    st.download_button("Download decision log CSV", data=decision_log.to_csv(index=False), file_name="decision_log.csv", mime="text/csv")

# ---------------------------
# Performance summary
# ---------------------------
def summarize_performance(d: pd.DataFrame, eq: pd.DataFrame) -> pd.DataFrame:
    ann_factor = 365
    pnl_cols = [
        "position_s1_pnl", "position_s2_pnl", "position_s3_pnl",
        "short_A_pnl", "short_B_pnl", "short_C_pnl",
        "bh_pnl"
    ]
    rows = []
    for col in pnl_cols:
        r = d[col].fillna(0.0)
        cum = r.sum() / 600.0  # normalize by starting capital
        vol = r.std() * np.sqrt(ann_factor)
        sharpe = (r.mean() * ann_factor) / (vol + 1e-12)
        curve = eq[col]
        mdd = (curve.cummax() - curve).max() / 600.0
        rows.append({"strategy": col.replace("_pnl",""), "cum_return": cum, "ann_vol": vol, "sharpe": sharpe, "max_drawdown": mdd})
    return pd.DataFrame(rows)

perf = summarize_performance(bt_row, equity_curves)
st.subheader("Performance summary (long and short)")
st.dataframe(
    perf.style.format({"cum_return": "{:.1%}", "ann_vol": "{:.2f}", "sharpe": "{:.2f}", "max_drawdown": "{:.1%}"}),
    use_container_width=True
)

st.info("Notes: Shorting small-cap crypto can be illiquid and costly. Borrow costs and slippage are simplified here.")
                
