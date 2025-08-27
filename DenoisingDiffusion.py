# app.py
# Low-Cap Crypto Crash Simulator & Backtest with Early Short-Entry Strategies
# - CoinGecko Pro key hard-coded
# - Monte Carlo crash probabilities
# - 3 long-side mitigation strategies (existing)
# - 3 early-detection short-entry strategies (new)
# - Decision logs and performance metrics

import streamlit as st
import pandas as pd
import numpy as np
import requests
from typing import Dict
from scipy.stats import norm

# ---------------------------
# Hard-coded CoinGecko Pro key (as requested)
# ---------------------------
COINGECKO_API_KEY = "CG-chRgqiH9ab4zsFTm2Zvst82a"
HEADERS: Dict[str, str] = {"x-cg-pro-api-key": COINGECKO_API_KEY} if COINGECKO_API_KEY else {}
COINGECKO_BASE = "https://api.coingecko.com/api/v3"

st.set_page_config(page_title="Low-Cap Crypto Crash Backtest (Long + Short)", layout="wide")
st.title("Low-Cap Crypto Sector Crash Simulator & Backtest (Long + Short)")

st.markdown("""
This dashboard simulates sector crash risk using liquidity and imbalance dynamics, and backtests:
- 3 long-side risk-mitigation strategies (reduce/exit when risk rises).
- 3 short-entry strategies with an early detection system.

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
    return out[["date","price","liquidity","imbalance","ret","volume","market_cap"]]

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
# Simulation parameters (from your model)
# ---------------------------
st.sidebar.header("Simulation Parameters")
H = st.sidebar.number_input("Simulation horizon (days)", value=10, min_value=2, max_value=180)
N_MC = st.sidebar.number_input("Monte Carlo paths", value=500, min_value=50, max_value=5000, step=50)
alpha_L = st.sidebar.number_input("alpha_L", value=0.05, step=0.01, format="%.2f")
beta_L = st.sidebar.number_input("beta_L", value=0.10, step=0.01, format="%.2f")
rho_I = st.sidebar.number_input("rho_I", value=0.95, step=0.01, format="%.2f")
mu_I = st.sidebar.number_input("mu_I", value=-0.02, step=0.01, format="%.2f")
sigma0 = st.sidebar.number_input("sigma0", value=0.03, step=0.005, format="%.3f")
kappa = st.sidebar.number_input("kappa", value=0.5, step=0.05, format="%.2f")
gamma = st.sidebar.number_input("gamma", value=0.5, step=0.05, format="%.2f")
crash_pct = st.sidebar.number_input("Crash % threshold", value=0.25, min_value=0.05, max_value=0.9, step=0.05, format="%.2f")
L_min_ratio = st.sidebar.number_input("Liquidity min ratio", value=0.10, min_value=0.01, max_value=1.0, step=0.01, format="%.2f")
I_crit = st.sidebar.number_input("Imbalance threshold", value=0.20, min_value=0.01, max_value=2.0, step=0.01, format="%.2f")
persist_days = st.sidebar.number_input("Persistence days (liquidity crash)", value=3, min_value=1, max_value=30)

def run_hybrid_simulation(P0, L0, I0, H, params):
    alpha_L, beta_L, rho_I, mu_I, sigma0, kappa, gamma = params
    P = P0
    L = max(1e-6, L0)
    I = np.clip(I0, -1, 1)
    path = []
    for _ in range(int(H)):
        delta_flow = 0.0
        r_prev = 0.0
        L_prop = L * (1 + alpha_L * delta_flow - beta_L * (abs(r_prev) > 0.05))
        shock = 0.0
        I_prop = rho_I * I + shock + np.random.randn() * 0.01
        gL = 1 + kappa * (1.0 / (L_prop + 1e-6)) ** gamma
        r = mu_I * I_prop + sigma0 * gL * np.random.randn()
        P *= np.exp(r)
        L = max(1e-6, L_prop)
        I = np.clip(I_prop, -1, 1)
        path.append([P, L, I, r])
    return np.array(path)

# ---------------------------
# Rolling crash probabilities (forecast series)
# ---------------------------
with st.expander("Compute rolling crash probabilities", expanded=True):
    progress = st.progress(0, text="Running Monte Carlo backtest...")
    crash_probs = []
    idxs = list(range(0, len(df) - int(H)))
    for i, idx in enumerate(idxs):
        P0 = df["price"].iloc[idx]; L0 = df["liquidity"].iloc[idx]; I0 = df["imbalance"].iloc[idx]
        crash_count = 0
        for _ in range(int(N_MC)):
            sim = run_hybrid_simulation(P0, L0, I0, int(H), [alpha_L, beta_L, rho_I, mu_I, sigma0, kappa, gamma])
            crash_price = sim[:,0] <= P0 * (1 - crash_pct)
            crash_liq = (sim[:,1] <= L_min_ratio * L0) & (sim[:,2] <= -I_crit)
            if persist_days <= len(crash_liq):
                crash_liq_persist = np.convolve(crash_liq.astype(int), np.ones(int(persist_days), dtype=int), "valid") >= persist_days
            else:
                crash_liq_persist = np.array([False])
            if np.any(crash_price) or np.any(crash_liq_persist):
                crash_count += 1
        crash_probs.append(crash_count / float(N_MC))
        progress.progress((i + 1) / len(idxs), text=f"Running... {i+1}/{len(idxs)}")

    df_bt = df.iloc[: len(crash_probs)].copy()
    df_bt["pred_crash_prob"] = crash_probs
    st.line_chart(df_bt.set_index("date")[["pred_crash_prob"]], use_container_width=True)

# ---------------------------
# Early Detection System (composite score for shorts)
# ---------------------------
st.subheader("Early Crash Detection System (for short entries)")

with st.sidebar.expander("Early detection parameters", expanded=True):
    ed_vol_window = st.number_input("Vol window (days)", value=14, min_value=5, max_value=60)
    ed_liq_window = st.number_input("Liquidity slope window", value=10, min_value=3, max_value=60)
    ed_prob_window = st.number_input("Crash prob slope window", value=10, min_value=3, max_value=60)
    ed_weight_prob = st.number_input("Weight: crash prob", value=0.5, min_value=0.0, max_value=1.0, step=0.05)
    ed_weight_liq = st.number_input("Weight: liquidity inverse", value=0.3, min_value=0.0, max_value=1.0, step=0.05)
    ed_weight_mom = st.number_input("Weight: negative momentum", value=0.2, min_value=0.0, max_value=1.0, step=0.05)
    ed_threshold = st.number_input("ECDS trigger threshold", value=0.6, min_value=0.0, max_value=2.0, step=0.05)

df_ed = df_bt.copy()
df_ed["ret"] = np.log(df_ed["price"]).diff()
df_ed["vol"] = df_ed["ret"].rolling(int(ed_vol_window)).std()
df_ed["liq_inv"] = 1.0 / (df_ed["liquidity"] + 1e-8)
df_ed["liq_slope"] = df_ed["liquidity"].diff(int(ed_liq_window)) / (np.abs(df_ed["liquidity"].shift(int(ed_liq_window))) + 1e-8)
df_ed["prob_slope"] = df_ed["pred_crash_prob"].diff(int(ed_prob_window)).fillna(0)
df_ed["neg_mom"] = (-df_ed["ret"].rolling(5).mean()).clip(lower=0)  # short-term negative drift
# Normalize components to 0-1 (robust)
for c in ["pred_crash_prob", "liq_inv", "liq_slope", "prob_slope", "neg_mom", "vol"]:
    x = df_ed[c].replace([np.inf, -np.inf], np.nan).fillna(0)
    p1, p99 = np.nanpercentile(x, [1, 99])
    x = (x - p1) / (p99 - p1 + 1e-9)
    df_ed[c + "_z"] = x.clip(0, 1)

# Composite Early Crash Detection Score (ECDS)
df_ed["ECDS"] = (
    ed_weight_prob * df_ed["pred_crash_prob_z"] +
    ed_weight_liq * df_ed["liq_inv_z"] +
    ed_weight_mom * df_ed["neg_mom_z"]
)
# Optional boost when vol and slopes are rising
df_ed["ECDS"] += 0.2 * df_ed["vol_z"] + 0.2 * df_ed["prob_slope_z"] + 0.1 * df_ed["liq_slope_z"]
st.line_chart(df_ed.set_index("date")[["ECDS"]], use_container_width=True)

# ---------------------------
# Long strategies (existing 3)
# ---------------------------
st.subheader("Long-side strategies")
with st.sidebar.expander("Long strategy parameters", expanded=True):
    # Strategy 1
    T1 = st.number_input("S1: Crash prob threshold", value=0.30, min_value=0.0, max_value=1.0, step=0.05)
    q_liq = st.number_input("S1: Liquidity percentile q", value=0.20, min_value=0.0, max_value=1.0, step=0.05)
    # Strategy 2
    k_mom = st.number_input("S2: Momentum lookback", value=20, min_value=5, max_value=120, step=5)
    T2 = st.number_input("S2: Crash prob soft threshold", value=0.25, min_value=0.0, max_value=1.0, step=0.05)
    reduce_factor = st.number_input("S2: Size reduce factor", value=0.5, min_value=0.0, max_value=1.0, step=0.1)
    # Strategy 3
    T3 = st.number_input("S3: Posterior mean threshold", value=0.35, min_value=0.0, max_value=1.0, step=0.05)
    cred_level = st.number_input("S3: Credible level", value=0.95, min_value=0.5, max_value=0.999, step=0.01)

df_long = df_ed.copy()
df_long["mom_k"] = df_long["price"] / df_long["price"].shift(int(k_mom)) - 1.0
liq_thresh = df_long["liquidity"].quantile(q_liq)

def rolling_beta_posterior(probs: pd.Series, window: int = 30, a0: float = 1.0, b0: float = 1.0, cred: float = 0.95):
    means, lowers, uppers = [], [], []
    for i in range(len(probs)):
        lo = max(0, i - window + 1)
        p_slice = probs.iloc[lo : i + 1].fillna(0).clip(0, 1)
        succ = p_slice.sum()
        trials = len(p_slice)
        a = a0 + succ
        b = b0 + (trials - succ)
        mean = a / (a + b)
        var = (a * b) / (((a + b) ** 2) * (a + b + 1))
        z = norm.ppf(0.5 + cred / 2.0)
        se = np.sqrt(var)
        means.append(mean); lowers.append(mean - z * se); uppers.append(mean + z * se)
    return pd.DataFrame({"post_mean": means, "post_low": lowers, "post_high": uppers}, index=probs.index)

post_df = rolling_beta_posterior(df_long["pred_crash_prob"], window=30, cred=cred_level)

def build_long_positions(d: pd.DataFrame) -> pd.DataFrame:
    x = d.copy()
    x["position_s1"] = 1.0
    x["position_s2"] = 0.0
    x["position_s3"] = 1.0
    # S1: Liquidity Guardrail
    x.loc[(x["pred_crash_prob"] > T1) & (x["liquidity"] <= liq_thresh), "position_s1"] = 0.0
    # S2: Momentum + Liquidity Filter
    x.loc[(x["mom_k"] > 0) & (x["liquidity"] > x["liquidity"].median()), "position_s2"] = 1.0
    x.loc[x["pred_crash_prob"] > T2, "position_s2"] *= reduce_factor
    # S3: Bayesian threshold
    x["post_mean"] = post_df["post_mean"].reindex(x.index).values
    x.loc[x["post_mean"] > T3, "position_s3"] = 0.0
    return x

long_bt = build_long_positions(df_long)

# ---------------------------
# Short-entry strategies (new 3)
# ---------------------------
st.subheader("Short-entry strategies (early detection)")

with st.sidebar.expander("Short strategy parameters", expanded=True):
    # Short Strategy A: ECDS threshold + confirmation
    A_threshold = st.number_input("A: ECDS threshold", value=ed_threshold, min_value=0.0, max_value=2.0, step=0.05)
    A_confirm_prob = st.number_input("A: Min crash prob", value=0.30, min_value=0.0, max_value=1.0, step=0.05)
    A_confirm_liq_q = st.number_input("A: Max liquidity percentile (thin)", value=0.30, min_value=0.0, max_value=1.0, step=0.05)

    # Short Strategy B: Microstructure breakdown (vol/liquidity regime shift)
    B_vol_z = st.number_input("B: Volatility z-threshold (0-1 scaled)", value=0.7, min_value=0.0, max_value=1.0, step=0.05)
    B_liq_inv_z = st.number_input("B: Inverse Liquidity z-threshold", value=0.7, min_value=0.0, max_value=1.0, step=0.05)
    B_prob_slope_z = st.number_input("B: Crash prob slope z-threshold", value=0.6, min_value=0.0, max_value=1.0, step=0.05)

    # Short Strategy C: Bayesian crash signal
    C_thresh = st.number_input("C: Posterior mean crash prob", value=0.40, min_value=0.0, max_value=1.0, step=0.05)
    C_cred = st.number_input("C: Require credible level (lower bound > T)? 1=yes,0=no", value=0, min_value=0, max_value=1)

liq_q_thresh_A = df_long["liquidity"].quantile(A_confirm_liq_q)

def build_short_positions(d: pd.DataFrame) -> pd.DataFrame:
    x = d.copy()
    # Initialize with no short
    x["short_A"] = 0.0
    x["short_B"] = 0.0
    x["short_C"] = 0.0

    # A) ECDS threshold + confirmations
    cond_A = (x["ECDS"] >= A_threshold) & \
             (x["pred_crash_prob"] >= A_confirm_prob) & \
             (x["liquidity"] <= liq_q_thresh_A)
    x.loc[cond_A, "short_A"] = 1.0

    # B) Regime shift: high vol, thin liquidity, rising crash slope
    cond_B = (x["vol_z"] >= B_vol_z) & (x["liq_inv_z"] >= B_liq_inv_z) & (x["prob_slope_z"] >= B_prob_slope_z)
    x.loc[cond_B, "short_B"] = 1.0

    # C) Bayesian crash signal using posterior mean of crash prob
    # If C_cred==1, you could require the posterior lower bound exceed C_thresh (we approximated above if needed)
    x["post_mean"] = post_df["post_mean"].reindex(x.index).values
    if C_cred == 1 and "post_low" in post_df.columns:
        post_low = post_df["post_low"].reindex(x.index).values
        x.loc[(x["post_mean"] >= C_thresh) & (post_low >= C_thresh), "short_C"] = 1.0
    else:
        x.loc[(x["post_mean"] >= C_thresh), "short_C"] = 1.0

    return x

short_bt = build_short_positions(df_long)

# ---------------------------
# PnL computation (long and short)
# ---------------------------
def compute_pnl(long_df: pd.DataFrame, fee_bps: float = 5.0, borrow_bps_daily: float = 1.0):
    d = long_df.copy()
    d["ret"] = d["ret"].fillna(0.0)

    # Long strategies: PnL = position * ret - transaction cost
    for col in ["position_s1", "position_s2", "position_s3"]:
        pos = d[col].fillna(0.0).values
        pos_shift = np.roll(pos, 1); pos_shift[0] = 0.0
        turnover = np.abs(pos - pos_shift)
        tc = turnover * (fee_bps / 1e4)
        d[f"{col}_pnl"] = pos * d["ret"] - tc

    # Short strategies: PnL = position * (-ret) - transaction cost - borrow cost
    # Simple borrow cost model: borrow_bps_daily (in bps) per day when short is on
    borrow_cost = borrow_bps_daily / 1e4
    for scol in ["short_A", "short_B", "short_C"]:
        pos = short_bt[scol].reindex(d.index).fillna(0.0).values
        pos_shift = np.roll(pos, 1); pos_shift[0] = 0.0
        turnover = np.abs(pos - pos_shift)
        tc = turnover * (fee_bps / 1e4)
        d[f"{scol}_pnl"] = pos * (-d["ret"]) - tc - pos * borrow_cost

    # Benchmark: buy-and-hold long
    d["bh_pnl"] = d["ret"]

    # Equity curves (log-returns cumulated -> exp)
    eq = d.set_index("date")[[
        "position_s1_pnl", "position_s2_pnl", "position_s3_pnl",
        "short_A_pnl", "short_B_pnl", "short_C_pnl",
        "bh_pnl"
    ]].cumsum().apply(np.exp)
    return d, eq

bt_row, equity_curves = compute_pnl(long_bt)

st.subheader("Equity curves (normalized)")
st.line_chart(equity_curves, use_container_width=True)

# ---------------------------
# Decision log
# ---------------------------
def build_decision_log(d: pd.DataFrame) -> pd.DataFrame:
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

decision_log = build_decision_log(long_bt)
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
        "bh_
    ]
    rows = []
    for col in pnl_cols:
        r = d[col].fillna(0.0)
        cum = np.exp(r.cumsum().iloc[-1]) - 1.0
        vol = r.std() * np.sqrt(ann_factor)
        sharpe = (r.mean() * ann_factor) / (vol + 1e-12)
        curve = eq[col.replace("_pnl", "")]
        mdd = (curve.cummax() / curve - 1.0).max()
        rows.append({"strategy": col.replace("_pnl",""), "cum_return": cum, "ann_vol": vol, "sharpe": sharpe, "max_drawdown": mdd})
    return pd.DataFrame(rows)

perf = summarize_performance(bt_row, equity_curves)
st.subheader("Performance summary (long and short)"
st.dataframe(
    perf.style.format({"cum_return": "{:.1%}", "ann_vol": "{:.2f}", "sharpe": "{:.2f}", "max_drawdown": "{:.1%}"}),
    use_container_width=True


st.info("Notes: Shorting small-cap crypto can be illiquid and costly. Borrow costs and slippage are simplified here.")

# ==============================
# Chunk 2 — Backtesting Engine & Strategy Rules
# ==============================

# Constants for backtest
STARTING_BALANCE = 600.0
FEE_BPS = 5.0          # transaction fee in basis points for each rebalance
BORROW_BPS_DAILY = 1.0 # borrow cost in bps per day for shorts
SLIPPAGE_ETA = 0.02    # slippage scale factor for notional vs liquidity
SLIPPAGE_DELTA = 0.5   # slippage exponent

def build_long_strategies(df):
    """
    Construct three simple long-side strategies that reduce/exit on rising crash risk.
    Returns df with position fraction columns: position_s1, position_s2, position_s3
    - S1: Liquidity guardrail (exit if Liquidity low & CrashSignal=1)
    - S2: Momentum + reduce on risk (enter on positive momentum, reduce size if CrashSignal=1)
    - S3: Bayesian-style posterior threshold (simple rolling avg of crash prob proxy)
    """
    x = df.copy()
    n = len(x)
    # naive crash probability proxy: rolling mean of CrashSignal
    x["crash_prob_proxy"] = x["CrashSignal"].rolling(30, min_periods=1).mean().fillna(0.0)

    # S1: default 100% long, remove exposure when crash signal present or liquidity below quantile
    liq_q = x["Liquidity"].quantile(0.20)
    x["position_s1"] = 1.0
    x.loc[(x["CrashSignal"] == 1) | (x["Liquidity"] <= liq_q), "position_s1"] = 0.0

    # S2: momentum + size reduce
    mom_k = 20
    x["mom20"] = x["Close"] / x["Close"].shift(mom_k) - 1.0
    x["position_s2"] = 0.0
    x.loc[(x["mom20"] > 0) & (x["Liquidity"] > x["Liquidity"].median()), "position_s2"] = 1.0
    # reduce factor when crash risk high
    x.loc[x["crash_prob_proxy"] > 0.25, "position_s2"] *= 0.5

    # S3: rolling posterior (simple)
    x["post_mean"] = x["crash_prob_proxy"].rolling(60, min_periods=1).mean()
    x["position_s3"] = 1.0
    x.loc[x["post_mean"] > 0.30, "position_s3"] = 0.0

    return x

def build_short_strategies(df):
    """
    Construct three short-entry strategies using early detection signals.
    Returns df with short flags short_A, short_B, short_C where 1.0 means full short exposure.
    - A: ECDS-based (use CrashSignal + liquidity thin)
    - B: Microstructure breakdown (vol spike + liquidity drop)
    - C: Posterior mean-based short
    """
    x = df.copy()
    # A: require crash signal and liquidity thin (quantile)
    thin_q = x["Liquidity"].quantile(0.30)
    x["short_A"] = 0.0
    x.loc[(x["CrashSignal"] == 1) & (x["Liquidity"] <= thin_q), "short_A"] = 1.0

    # B: microstructure breakdown: recent vol spike + liquidity drop
    x["ret"] = np.log(x["Close"]).diff().fillna(0.0)
    x["vol_14"] = x["ret"].rolling(14, min_periods=1).std().fillna(0.0)
    # z-score like scaling
    vol_p1, vol_p99 = np.nanpercentile(x["vol_14"].fillna(0.0), [1,99])
    x["vol_z"] = ((x["vol_14"] - vol_p1) / (vol_p99 - vol_p1 + 1e-9)).clip(0,1)
    # liquidity inverse z
    liq_p1, liq_p99 = np.nanpercentile(1.0/(x["Liquidity"]+1e-9), [1,99])
    x["liq_inv_z"] = ((1.0/(x["Liquidity"]+1e-9) - liq_p1) / (liq_p99 - liq_p1 + 1e-9)).clip(0,1)
    x["prob_slope"] = x["CrashSignal"].diff(5).fillna(0.0)
    psl1, psl99 = np.nanpercentile(x["prob_slope"].fillna(0.0), [1,99])
    x["prob_slope_z"] = ((x["prob_slope"] - psl1) / (psl99 - psl1 + 1e-9)).clip(0,1)

    x["short_B"] = 0.0
    x.loc[(x["vol_z"] >= 0.7) & (x["liq_inv_z"] >= 0.7) & (x["prob_slope_z"] >= 0.6), "short_B"] = 1.0

    # C: posterior mean-based
    x["posterior_mean"] = x["CrashSignal"].rolling(30, min_periods=1).mean()
    x["short_C"] = 0.0
    x.loc[x["posterior_mean"] >= 0.40, "short_C"] = 1.0

    return x

def slippage_model(notional, liquidity):
    """
    Simple slippage model:
    slippage_bps = eta * (notional / liquidity)^delta
    We cap slippage to e.g. 50% of price (extreme).
    Returns fractional price impact (positive = cost)
    """
    if liquidity <= 0:
        return 1.0  # total loss extreme fallback
    ratio = notional / (liquidity + 1e-9)
    slippage = SLIPPAGE_ETA * (ratio ** SLIPPAGE_DELTA)
    return float(np.clip(slippage, 0.0, 0.5))

def compute_pnl_engine(df_long, df_short, start_balance=STARTING_BALANCE,
                       fee_bps=FEE_BPS, borrow_bps_daily=BORROW_BPS_DAILY,
                       include_slippage=True):
    """
    Core PnL engine. Each strategy (three long, three short, benchmark) starts with `start_balance` dollars.
    Position fractions are applied to current equity each day. Transaction costs and borrow costs applied.
    Returns:
      - df_out: df with position columns and daily equity columns for each strategy (eq_position_s1, etc)
      - equity_curves: DataFrame indexed by date with dollar balances per strategy
    """
    d = df_long.copy().reset_index(drop=True)
    s = df_short.copy().reset_index(drop=True)
    n = len(d)
    dates = d["date"].values

    long_cols = ["position_s1", "position_s2", "position_s3"]
    short_cols = ["short_A", "short_B", "short_C"]

    equity = {col: np.zeros(n, dtype=float) for col in long_cols + short_cols + ["bh"]}
    equity_prev = {col: float(start_balance) for col in equity.keys()}

    # Initialize prev position fractions
    prev_pos = {col: 0.0 for col in long_cols + short_cols}

    for t in range(n):
        ret_t = float(d.at[t, "ret"]) if "ret" in d.columns else float(np.log(d.at[t, "Close"]) - np.log(d.at[t-1, "Close"])) if t>0 else 0.0

        # Long strategies
        for col in long_cols:
            pos_frac = float(d.at[t, col]) if col in d.columns else 0.0
            pos_prev = prev_pos[col]
            turnover = abs(pos_frac - pos_prev)
            notional_change = turnover * equity_prev[col]
            # slippage cost as fraction of price (converted to dollar)
            slippage_cost = 0.0
            if include_slippage and notional_change > 0:
                slippage_frac = slippage_model(notional_change, d.at[t, "Liquidity"])
                slippage_cost = slippage_frac * notional_change
            tc = turnover * (fee_bps / 1e4) * equity_prev[col] + slippage_cost
            pnl = pos_frac * equity_prev[col] * ret_t - tc
            equity[col][t] = equity_prev[col] + pnl
            equity_prev[col] = equity[col][t]
            prev_pos[col] = pos_frac

        # Short strategies
        borrow_cost_frac = borrow_bps_daily / 1e4
        for scol in short_cols:
            pos_frac = float(s.at[t, scol]) if scol in s.columns else 0.0
            pos_prev = prev_pos[scol]
            turnover = abs(pos_frac - pos_prev)
            notional_change = turnover * equity_prev[scol]
            slippage_cost = 0.0
            if include_slippage and notional_change > 0:
                slippage_frac = slippage_model(notional_change, d.at[t, "Liquidity"])
                slippage_cost = slippage_frac * notional_change
            tc = turnover * (fee_bps / 1e4) * equity_prev[scol] + slippage_cost
            pnl = pos_frac * equity_prev[scol] * (-ret_t) - tc - pos_frac * equity_prev[scol] * borrow_cost_frac
            equity[scol][t] = equity_prev[scol] + pnl
            equity_prev[scol] = equity[scol][t]
            prev_pos[scol] = pos_frac

        # Buy & hold benchmark (compounded)
        equity["bh"][t] = equity_prev["bh"] * np.exp(ret_t)
        equity_prev["bh"] = equity["bh"][t]

    # Build equity curves DataFrame
    equity_curves = pd.DataFrame({k: equity[k] for k in equity.keys()}, index=dates)
    equity_curves.index.name = "date"

    # Attach equity columns to d_out for decision logs
    d_out = d.copy()
    for col in equity_curves.columns:
        d_out[f"eq_{col}"] = equity_curves[col].values

    return d_out, equity_curves

def performance_summary(equity_curves, ann_factor=365):
    rows = []
    for col in equity_curves.columns:
        ser = equity_curves[col].dropna()
        if len(ser) < 2:
            rows.append({"strategy": col, "cum_return": np.nan, "ann_vol": np.nan, "sharpe": np.nan, "max_drawdown": np.nan})
            continue
        daily_ret = ser.pct_change().fillna(0.0)
        cum_return = (ser.iloc[-1] / ser.iloc[0]) - 1.0
        ann_vol = daily_ret.std(ddof=0) * np.sqrt(ann_factor)
        ann_mean = daily_ret.mean() * ann_factor
        sharpe = (ann_mean / (ann_vol + 1e-12)) if ann_vol > 0 else np.nan
        roll_max = ser.cummax()
        max_dd = (ser / roll_max - 1.0).min()
        rows.append({"strategy": col, "cum_return": cum_return, "ann_vol": ann_vol, "sharpe": sharpe, "max_drawdown": max_dd})
    return pd.DataFrame(rows)
# ==============================
# Chunk 3 — Streamlit UI, Charts & Downloads
# (Paste after Chunk 1 & Chunk 2)
# ==============================

# ---------- Sidebar controls for crash detection ----------
st.sidebar.header("Crash Detection Controls")
liq_threshold = st.sidebar.slider("Liquidity drop threshold (relative)", min_value=0.01, max_value=1.0, value=0.30, step=0.01)
sd_threshold = st.sidebar.slider("Supply-Demand threshold (negative = selling)", min_value=-1.0, max_value=0.0, value=-0.20, step=0.01)
run_button = st.sidebar.button("Run Backtest & Compute")

# ---------- Input for symbol and date range ----------
st.sidebar.header("Data Selection (yfinance)")
symbol = st.sidebar.text_input("Ticker (yfinance)", value="BTC-USD")
end_date = st.sidebar.date_input("End date", value=datetime.utcnow().date())
start_date = st.sidebar.date_input("Start date", value=(end_date - timedelta(days=365)))
if start_date >= end_date:
    st.sidebar.error("Start date must be before end date.")

# ---------- Run pipeline ----------
if run_button:
    with st.spinner("Downloading data and building signals..."):
        try:
            raw_df = download_data(symbol, start=start_date, end=end_date + timedelta(days=1))
        except Exception as e:
            st.error(f"Data download failed: {e}")
            st.stop()

        if raw_df.empty:
            st.error("No data returned for symbol / date-range.")
            st.stop()

        # Rename to match expected columns in other chunks
        raw_df = raw_df.reset_index().rename(columns={"Date": "date", "Adj Close": "Close"})
        # Ensure Open column exists (yfinance has it)
        if "Open" not in raw_df.columns:
            raw_df["Open"] = raw_df["Close"].shift(1).fillna(method="bfill")

        # Compute liquidity & supply-demand (uses 600 window by default to match earlier spec)
        df_proc = compute_liquidity(raw_df.copy(), window=600)
        df_proc = compute_supply_demand(df_proc.copy(), window=600)

        # Detect crash signals using user-controlled thresholds
        df_proc = detect_crash_signal(df_proc.copy(), liq_threshold=liq_threshold, sd_threshold=sd_threshold)

        # Build strategy exposures
        df_long = build_long_strategies(df_proc.copy())
        df_short = build_short_strategies(df_proc.copy())

        # Compute PnL & equity curves
        bt_out, equity_curves = compute_pnl_engine(df_long, df_short, start_balance=STARTING_BALANCE,
                                                   fee_bps=FEE_BPS, borrow_bps_daily=BORROW_BPS_DAILY,
                                                   include_slippage=True)

    # ---------- Main layout: top row ----------
    st.markdown("## Price, Liquidity & Crash Signals")
    col1, col2 = st.columns([2, 1])

    # Price + CrashSignal
    with col1:
        fig, ax = plt.subplots(2, 1, figsize=(12, 6), sharex=True,
                               gridspec_kw={"height_ratios": [3, 1]})
        ax[0].plot(df_proc["date"], df_proc["Close"], label=f"{symbol} Price")
        ax[0].set_ylabel("Price")
        ax[0].legend(loc="upper left")
        # highlight crash-signal days
        sig_dates = df_proc.loc[df_proc["CrashSignal"] == 1, "date"]
        if not sig_dates.empty:
            ax[0].scatter(sig_dates, df_proc.loc[df_proc["CrashSignal"] == 1, "Close"], color="red", label="CrashSignal", zorder=5)
            ax[0].legend()

        # Liquidity plot
        ax[1].plot(df_proc["date"], df_proc["Liquidity"], label="Liquidity (proxy)")
        ax[1].axhline(df_proc["Liquidity"].quantile(0.2), color="orange", linestyle="--", label="20%-liq-quantile")
        ax[1].set_ylabel("Liquidity")
        ax[1].legend(loc="upper left")

        st.pyplot(fig)

    # Quick stats in right column
    with col2:
        st.write("### Snapshot")
        last = df_proc.iloc[-1]
        st.metric("Price (last)", f"{last['Close']:.4f}")
        st.metric("Liquidity (last)", f"{last['Liquidity']:.2e}")
        st.metric("Supply-Demand (last)", f"{last['SupplyDemand']:.4f}")
        st.markdown("**CrashSignal (last)**: " + ("YES" if last["CrashSignal"] == 1 else "no"))

    # ---------- Equity curves and performance ----------
    st.markdown("## Equity Curves (each strategy starts with $600)")
    st.line_chart(equity_curves, use_container_width=True)

    perf_df = performance_summary(equity_curves)
    st.markdown("### Performance Summary")
    st.dataframe(perf_df.style.format({
        "cum_return": "{:.2%}",
        "ann_vol": "{:.2f}",
        "sharpe": "{:.2f}",
        "max_drawdown": "{:.2%}"
    }), use_container_width=True)

    # ---------- Decision log & downloads ----------
    st.markdown("## Decision Log and Exports")
    decision_log = bt_out[[
        "date", "Close", "Liquidity", "SupplyDemand", "CrashSignal",
        "position_s1", "position_s2", "position_s3",
        "short_A", "short_B", "short_C"
    ]].copy()
    # Add forward return & crash flag
    decision_log[f"fwd_{int(H)}d_return"] = bt_out["Close"].shift(-int(H)) / bt_out["Close"] - 1.0
    decision_log[f"fwd_{int(H)}d_crash"] = (decision_log[f"fwd_{int(H)}d_return"] <= -crash_pct).astype(int)

    st.dataframe(decision_log.tail(50), use_container_width=True)
    csv_decision = decision_log.to_csv(index=False)
    st.download_button("Download decision log CSV", data=csv_decision, file_name="decision_log.csv", mime="text/csv")

    csv_eq = equity_curves.reset_index().to_csv(index=False)
    st.download_button("Download equity curves CSV", data=csv_eq, file_name="equity_curves.csv", mime="text/csv")

    # ---------- Additional visualizations ----------
    st.markdown("## Signal Diagnostics")
    col3, col4 = st.columns(2)
    with col3:
        st.line_chart(df_proc.set_index("date")[["Liquidity"]].rename(columns={"Liquidity": "Liquidity (proxy)"}), use_container_width=True)
    with col4:
        st.bar_chart(decision_log.set_index("date")[[f"fwd_{int(H)}d_crash"]].astype(int), use_container_width=True)

    st.success("Backtest complete. Use sidebar to tweak thresholds or date range and re-run.")

else:
    st.info("Set parameters and click **Run Backtest & Compute** in the sidebar.")

# ==============================
# Chunk 3 — Streamlit UI, Charts & Downloads
# (Paste after Chunk 1 & Chunk 2)
# ==============================

# ---------- Sidebar controls for crash detection ----------
st.sidebar.header("Crash Detection Controls")
liq_threshold = st.sidebar.slider("Liquidity drop threshold (relative)", min_value=0.01, max_value=1.0, value=0.30, step=0.01)
sd_threshold = st.sidebar.slider("Supply-Demand threshold (negative = selling)", min_value=-1.0, max_value=0.0, value=-0.20, step=0.01)
run_button = st.sidebar.button("Run Backtest & Compute")

# ---------- Input for symbol and date range ----------
st.sidebar.header("Data Selection (yfinance)")
symbol = st.sidebar.text_input("Ticker (yfinance)", value="BTC-USD")
end_date = st.sidebar.date_input("End date", value=datetime.utcnow().date())
start_date = st.sidebar.date_input("Start date", value=(end_date - timedelta(days=365)))
if start_date >= end_date:
    st.sidebar.error("Start date must be before end date.")

# ---------- Run pipeline ----------
if run_button:
    with st.spinner("Downloading data and building signals..."):
        try:
            raw_df = download_data(symbol, start=start_date, end=end_date + timedelta(days=1))
        except Exception as e:
            st.error(f"Data download failed: {e}")
            st.stop()

        if raw_df.empty:
            st.error("No data returned for symbol / date-range.")
            st.stop()

        # Rename to match expected columns in other chunks
        raw_df = raw_df.reset_index().rename(columns={"Date": "date", "Adj Close": "Close"})
        # Ensure Open column exists (yfinance has it)
        if "Open" not in raw_df.columns:
            raw_df["Open"] = raw_df["Close"].shift(1).fillna(method="bfill")

        # Compute liquidity & supply-demand (uses 600 window by default to match earlier spec)
        df_proc = compute_liquidity(raw_df.copy(), window=600)
        df_proc = compute_supply_demand(df_proc.copy(), window=600)

        # Detect crash signals using user-controlled thresholds
        df_proc = detect_crash_signal(df_proc.copy(), liq_threshold=liq_threshold, sd_threshold=sd_threshold)

        # Build strategy exposures
        df_long = build_long_strategies(df_proc.copy())
        df_short = build_short_strategies(df_proc.copy())

        # Compute PnL & equity curves
        bt_out, equity_curves = compute_pnl_engine(df_long, df_short, start_balance=STARTING_BALANCE,
                                                   fee_bps=FEE_BPS, borrow_bps_daily=BORROW_BPS_DAILY,
                                                   include_slippage=True)

    # ---------- Main layout: top row ----------
    st.markdown("## Price, Liquidity & Crash Signals")
    col1, col2 = st.columns([2, 1])

    # Price + CrashSignal
    with col1:
        fig, ax = plt.subplots(2, 1, figsize=(12, 6), sharex=True,
                               gridspec_kw={"height_ratios": [3, 1]})
        ax[0].plot(df_proc["date"], df_proc["Close"], label=f"{symbol} Price")
        ax[0].set_ylabel("Price")
        ax[0].legend(loc="upper left")
        # highlight crash-signal days
        sig_dates = df_proc.loc[df_proc["CrashSignal"] == 1, "date"]
        if not sig_dates.empty:
            ax[0].scatter(sig_dates, df_proc.loc[df_proc["CrashSignal"] == 1, "Close"], color="red", label="CrashSignal", zorder=5)
            ax[0].legend()

        # Liquidity plot
        ax[1].plot(df_proc["date"], df_proc["Liquidity"], label="Liquidity (proxy)")
        ax[1].axhline(df_proc["Liquidity"].quantile(0.2), color="orange", linestyle="--", label="20%-liq-quantile")
        ax[1].set_ylabel("Liquidity")
        ax[1].legend(loc="upper left")

        st.pyplot(fig)

    # Quick stats in right column
    with col2:
        st.write("### Snapshot")
        last = df_proc.iloc[-1]
        st.metric("Price (last)", f"{last['Close']:.4f}")
        st.metric("Liquidity (last)", f"{last['Liquidity']:.2e}")
        st.metric("Supply-Demand (last)", f"{last['SupplyDemand']:.4f}")
        st.markdown("**CrashSignal (last)**: " + ("YES" if last["CrashSignal"] == 1 else "no"))

    # ---------- Equity curves and performance ----------
    st.markdown("## Equity Curves (each strategy starts with $600)")
    st.line_chart(equity_curves, use_container_width=True)

    perf_df = performance_summary(equity_curves)
    st.markdown("### Performance Summary")
    st.dataframe(perf_df.style.format({
        "cum_return": "{:.2%}",
        "ann_vol": "{:.2f}",
        "sharpe": "{:.2f}",
        "max_drawdown": "{:.2%}"
    }), use_container_width=True)

    # ---------- Decision log & downloads ----------
    st.markdown("## Decision Log and Exports")
    decision_log = bt_out[[
        "date", "Close", "Liquidity", "SupplyDemand", "CrashSignal",
        "position_s1", "position_s2", "position_s3",
        "short_A", "short_B", "short_C"
    ]].copy()
    # Add forward return & crash flag
    decision_log[f"fwd_{int(H)}d_return"] = bt_out["Close"].shift(-int(H)) / bt_out["Close"] - 1.0
    decision_log[f"fwd_{int(H)}d_crash"] = (decision_log[f"fwd_{int(H)}d_return"] <= -crash_pct).astype(int)

    st.dataframe(decision_log.tail(50), use_container_width=True)
    csv_decision = decision_log.to_csv(index=False)
    st.download_button("Download decision log CSV", data=csv_decision, file_name="decision_log.csv", mime="text/csv")

    csv_eq = equity_curves.reset_index().to_csv(index=False)
    st.download_button("Download equity curves CSV", data=csv_eq, file_name="equity_curves.csv", mime="text/csv")

    # ---------- Additional visualizations ----------
    st.markdown("## Signal Diagnostics")
    col3, col4 = st.columns(2)
    with col3:
        st.line_chart(df_proc.set_index("date")[["Liquidity"]].rename(columns={"Liquidity": "Liquidity (proxy)"}), use_container_width=True)
    with col4:
        st.bar_chart(decision_log.set_index("date")[[f"fwd_{int(H)}d_crash"]].astype(int), use_container_width=True)

    st.success("Backtest complete. Use sidebar to tweak thresholds or date range and re-run.")

else:
    st.info("Set parameters and click **Run Backtest & Compute** in the sidebar.")
    
