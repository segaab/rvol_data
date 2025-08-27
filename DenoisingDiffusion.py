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
    # Keep canonical names: date, price, liquidity, imbalance, ret, volume, market_cap
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
    # sensible defaults (IDs)
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
        # assume already contains price/liquidity/imbalance columns; if not try to derive
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
persist_days = st.sidebar.number_input("Liquidity persistence days", value=3, min_value=1, max_value=30)

# ---------------------------
# Hybrid forward simulator
# ---------------------------
def run_hybrid_simulation(P0, L0, I0, H, params):
    alpha_L, beta_L, rho_I, mu_I, sigma0, kappa, gamma = params
    P = float(P0)
    L = float(max(1e-12, L0))
    I = float(np.clip(I0, -1.0, 1.0))
    path = []
    for _ in range(int(H)):
        delta_flow = 0.0
        r_prev = 0.0
        L_prop = L * (1.0 + alpha_L * delta_flow - beta_L * (1.0 if abs(r_prev) > 0.05 else 0.0))
        shock = 0.0
        I_prop = rho_I * I + shock + np.random.randn() * 0.01
        gL = 1.0 + kappa * (1.0 / (L_prop + 1e-12)) ** gamma
        r = mu_I * I_prop + sigma0 * gL * np.random.randn()
        P = P * np.exp(r)
        L = max(1e-12, L_prop)
        I = np.clip(I_prop, -1.0, 1.0)
        path.append((P, L, I, r))
    return np.array(path, dtype=float)

# ---------------------------
# Rolling Monte Carlo crash probability: compute once
# ---------------------------
if st.button("Run Monte Carlo rolling crash probabilities"):
    with st.spinner("Running Monte Carlo..."):
        idxs = list(range(0, len(df)-int(H)))
        crash_probs = np.zeros(len(idxs))
        total = len(idxs)
        for i, idx in enumerate(idxs):
            P0 = df["price"].iloc[idx]
            L0 = df["liquidity"].iloc[idx]
            I0 = df["imbalance"].iloc[idx]
            crash_count = 0
            for _ in range(int(N_MC)):
                sim = run_hybrid_simulation(P0, L0, I0, int(H),
                                           [alpha_L, beta_L, rho_I, mu_I, sigma0, kappa, gamma])
                crash_price = np.any(sim[:, 0] <= P0 * (1.0 - float(crash_pct)))
                crash_liq = (sim[:, 1] <= float(L_min_ratio) * float(L0)) & (sim[:, 2] <= -float(I_crit))
                if len(crash_liq) >= int(persist_days):
                    crash_liq_persist = np.convolve(crash_liq.astype(int), np.ones(int(persist_days), dtype=int), "valid") >= persist_days
                    crash_liq_flag = np.any(crash_liq_persist)
                else:
                    crash_liq_flag = False
                if crash_price or crash_liq_flag:
                    crash_count += 1
            crash_probs[i] = crash_count / float(N_MC)
            if (i % 10) == 0:
                st.progress((i+1)/total)
        df_sim = df.iloc[:len(crash_probs)].copy().reset_index(drop=True)
        df_sim["crash_prob"] = crash_probs
        st.success("Monte Carlo simulation complete.")
        st.line_chart(df_sim.set_index("date")[["crash_prob"]], use_container_width=True)
else:
    # If not run, try to create a minimal df_sim with crash_prob = 0 (so other code can run)
    df_sim = df.copy().reset_index(drop=True)
    df_sim["crash_prob"] = 0.0

# ---------------------------
# Early Crash Detection Score (ECDS)
# ---------------------------
st.subheader("Early Crash Detection System (ECDS)")

with st.sidebar.expander("ECDS parameters", expanded=False):
    ed_vol_window = st.number_input("ECDS vol window (days)", value=14, min_value=5, max_value=60)
    ed_liq_window = st.number_input("ECDS liquidity slope window", value=10, min_value=3, max_value=60)
    ed_prob_window = st.number_input("ECDS prob slope window", value=10, min_value=3, max_value=60)
    ed_weight_prob = st.number_input("Weight: crash prob", value=0.5, min_value=0.0, max_value=1.0, step=0.05)
    ed_weight_liq = st.number_input("Weight: liquidity inverse", value=0.3, min_value=0.0, max_value=1.0, step=0.05)
    ed_weight_mom = st.number_input("Weight: negative momentum", value=0.2, min_value=0.0, max_value=1.0, step=0.05)
    ed_threshold = st.number_input("ECDS trigger threshold", value=0.6, min_value=0.0, max_value=2.0, step=0.05)

df_ed = df_sim.copy().reset_index(drop=True)
df_ed["ret"] = np.log(df_ed["price"]).diff().fillna(0.0)
df_ed["vol"] = df_ed["ret"].rolling(ed_vol_window, min_periods=1).std().fillna(0.0)
df_ed["liq_inv"] = 1.0 / (df_ed["liquidity"] + 1e-12)
df_ed["liq_slope"] = df_ed["liquidity"].diff(ed_liq_window).fillna(0.0) / (np.abs(df_ed["liquidity"].shift(ed_liq_window).fillna(method="bfill")) + 1e-12)
df_ed["prob_slope"] = df_ed["crash_prob"].diff(ed_prob_window).fillna(0.0)
df_ed["neg_mom"] = (-df_ed["ret"].rolling(5, min_periods=1).mean()).clip(lower=0.0)

# robust 0-1 scaling per feature
def robust_01(x):
    x = np.asarray(x)
    p1 = np.nanpercentile(x, 1)
    p99 = np.nanpercentile(x, 99)
    denom = (p99 - p1) if (p99 - p1) > 0 else 1.0
    z = (x - p1) / denom
    z = np.clip(z, 0.0, 1.0)
    return z

df_ed["crash_prob_z"] = robust_01(df_ed["crash_prob"].fillna(0.0))
df_ed["liq_inv_z"] = robust_01(df_ed["liq_inv"].fillna(0.0))
df_ed["liq_slope_z"] = robust_01(df_ed["liq_slope"].fillna(0.0))
df_ed["prob_slope_z"] = robust_01(df_ed["prob_slope"].fillna(0.0))
df_ed["neg_mom_z"] = robust_01(df_ed["neg_mom"].fillna(0.0))
df_ed["vol_z"] = robust_01(df_ed["vol"].fillna(0.0))

df_ed["ECDS"] = (ed_weight_prob * df_ed["crash_prob_z"] +
                 ed_weight_liq * df_ed["liq_inv_z"] +
                 ed_weight_mom * df_ed["neg_mom_z"])
# small boosts for volatility & slopes
df_ed["ECDS"] = df_ed["ECDS"] + 0.2 * df_ed["vol_z"] + 0.2 * df_ed["prob_slope_z"] + 0.1 * df_ed["liq_slope_z"]
st.line_chart(df_ed.set_index("date")[["ECDS"]], use_container_width=True)

# ---------------------------
# Long strategy definitions
# ---------------------------
st.subheader("Long strategies (risk mitigation)")

with st.sidebar.expander("Long strategy params", expanded=False):
    T1 = st.number_input("S1 crash prob threshold", value=0.30)
    q_liq = st.number_input("S1 liquidity quantile (exit)", value=0.20)
    k_mom = st.number_input("S2 momentum lookback", value=20)
    T2 = st.number_input("S2 soft crash prob threshold (reduce)", value=0.25)
    reduce_factor = st.number_input("S2 reduce factor", value=0.5, min_value=0.0, max_value=1.0, step=0.05)
    T3 = st.number_input("S3 posterior mean threshold (exit)", value=0.35)
    cred_level = st.number_input("S3 credible level", value=0.95)

df_long = df_ed.copy()
df_long["mom_k"] = df_long["price"] / df_long["price"].shift(int(k_mom)) - 1.0
liq_thresh = df_long["liquidity"].quantile(q_liq)

def rolling_beta_posterior(probs: pd.Series, window: int = 30, a0: float = 1.0, b0: float = 1.0, cred: float = 0.95) -> pd.DataFrame:
    means, lowers, uppers = [], [], []
    for i in range(len(probs)):
        lo = max(0, i - window + 1)
        p_slice = probs.iloc[lo:i+1].fillna(0).clip(0,1)
        succ = p_slice.sum()
        trials = len(p_slice)
        a = a0 + succ
        b = b0 + (trials - succ)
        mean = a / (a + b)
        var = (a * b) / (((a + b) ** 2) * (a + b + 1))
        z = norm.ppf(0.5 + cred / 2.0)
        se = np.sqrt(var) if var > 0 else 0.0
        means.append(mean)
        lowers.append(mean - z * se)
        uppers.append(mean + z * se)
    return pd.DataFrame({"post_mean": means, "post_low": lowers, "post_high": uppers}, index=probs.index)

post_df = rolling_beta_posterior(df_long["crash_prob"], window=30, cred=cred_level)

def build_long_positions(df_in: pd.DataFrame) -> pd.DataFrame:
    x = df_in.copy()
    # positions are fractions of capital (0..1)
    x["position_s1"] = 1.0
    x["position_s2"] = 0.0
    x["position_s3"] = 1.0
    # S1: guardrail - exit if crash prob high and liquidity low
    x.loc[(x["crash_prob"] > T1) & (x["liquidity"] <= liq_thresh), "position_s1"] = 0.0
    # S2: momentum + reduce on risk
    x.loc[(x["mom_k"] > 0) & (x["liquidity"] > x["liquidity"].median()), "position_s2"] = 1.0
    x.loc[x["crash_prob"] > T2, "position_s2"] *= reduce_factor
    # S3: posterior mean threshold
    x["post_mean"] = post_df["post_mean"].reindex(x.index).values
    x.loc[x["post_mean"] > T3, "position_s3"] = 0.0
    return x

long_bt = build_long_positions(df_long)

# ---------------------------
# Short strategy definitions
# ---------------------------
st.subheader("Short strategies (early detection)")

with st.sidebar.expander("Short strategy params", expanded=False):
    A_threshold = st.number_input("A: ECDS threshold", value=ed_threshold)
    A_confirm_prob = st.number_input("A: min crash prob", value=0.30)
    A_confirm_liq_q = st.number_input("A: max liquidity percentile (thin)", value=0.30)
    B_vol_z = st.number_input("B: vol z-threshold", value=0.7)
    B_liq_inv_z = st.number_input("B: liq inv z-threshold", value=0.7)
    B_prob_slope_z = st.number_input("B: prob slope z-threshold", value=0.6)
    C_thresh = st.number_input("C: posterior mean crash prob", value=0.40)
    C_cred = st.number_input("C: require credible lower bound", value=0, min_value=0, max_value=1)

liq_q_thresh_A = df_long["liquidity"].quantile(A_confirm_liq_q)

def build_short_positions(df_in: pd.DataFrame) -> pd.DataFrame:
    x = df_in.copy()
    x["short_A"] = 0.0
    x["short_B"] = 0.0
    x["short_C"] = 0.0
    # A: ECDS + confirmation + thin liquidity
    cond_A = (x["ECDS"] >= A_threshold) & (x["crash_prob"] >= A_confirm_prob) & (x["liquidity"] <= liq_q_thresh_A)
    x.loc[cond_A, "short_A"] = 1.0
    # B: microstructure regime shift
    # compute z features if missing
    if "vol_z" not in x.columns:
        x["vol_z"] = robust_01(x["ret"].rolling(ed_vol_window, min_periods=1).std().fillna(0.0))
    if "liq_inv_z" not in x.columns:
        x["liq_inv_z"] = robust_01(1.0 / (x["liquidity"] + 1e-12))
    if "prob_slope_z" not in x.columns:
        x["prob_slope_z"] = robust_01(x["crash_prob"].diff(ed_prob_window).fillna(0.0))
    cond_B = (x["vol_z"] >= B_vol_z) & (x["liq_inv_z"] >= B_liq_inv_z) & (x["prob_slope_z"] >= B_prob_slope_z)
    x.loc[cond_B, "short_B"] = 1.0
    # C: posterior mean discriminator
    x["post_mean"] = post_df["post_mean"].reindex(x.index).values
    if C_cred == 1 and "post_low" in post_df.columns:
        post_low = post_df["post_low"].reindex(x.index).values
        x.loc[(x["post_mean"] >= C_thresh) & (post_low >= C_thresh), "short_C"] = 1.0
    else:
        x.loc[(x["post_mean"] >= C_thresh), "short_C"] = 1.0
    return x

short_bt = build_short_positions(df_ed)

# ---------------------------
# PnL engine
# ---------------------------
def slippage_model(notional: float, liquidity: float, eta: float = 0.02, delta: float = 0.5) -> float:
    """Return fractional slippage cost (0..1) for the notional given liquidity proxy."""
    if liquidity <= 0:
        return 1.0
    ratio = notional / (liquidity + 1e-12)
    slippage = eta * (ratio ** delta)
    return float(np.clip(slippage, 0.0, 0.5))

def compute_pnl(long_df: pd.DataFrame, short_df: pd.DataFrame,
                start_cap: float = START_BALANCE,
                fee_bps: float = 5.0,
                borrow_bps_daily: float = 1.0,
                include_slippage: bool = True):
    d = long_df.copy().reset_index(drop=True)
    s = short_df.copy().reset_index(drop=True)
    n = len(d)
    dates = d["date"].values

    long_cols = ["position_s1", "position_s2", "position_s3"]
    short_cols = ["short_A", "short_B", "short_C"]

    # equity per strategy (dollar balance time series)
    equity = {name: np.zeros(n, dtype=float) for name in ["long_s1", "long_s2", "long_s3", "short_a", "short_b", "short_c", "bh"]}
    # initialize
    for k in equity.keys():
        equity[k][0] = start_cap

    prev_pos = {col: 0.0 for col in long_cols + short_cols}

    for t in range(n):
        ret_t = float(d.at[t, "ret"]) if "ret" in d.columns else (np.log(d.at[t, "price"]) - np.log(d.at[t-1, "price"])) if t>0 else 0.0
        liq_t = float(d.at[t, "liquidity"]) if "liquidity" in d.columns else 0.0

        # Long strategies (apply fraction of capital)
        for idx, col in enumerate(long_cols, start=1):
            pos_frac = float(d.at[t, col]) if col in d.columns else 0.0
            pos_prev = prev_pos[col]
            turnover = abs(pos_frac - pos_prev)
            capital = equity[f"long_s{idx}"][t-1] if t>0 else start_cap
            tc = turnover * (fee_bps / 1e4) * capital
            slippage_cost = 0.0
            if include_slippage and turnover > 0:
                notional_change = turnover * capital
                slippage_cost = slippage_model(notional_change, liq_t) * notional_change
            pnl = pos_frac * capital * ret_t - tc - slippage_cost
            equity[f"long_s{idx}"][t] = (equity[f"long_s{idx}"][t-1] if t>0 else start_cap) + pnl
            prev_pos[col] = pos_frac

        # Short strategies
        borrow_frac = borrow_bps_daily / 1e4
        for idx, scol in enumerate(short_cols, start=1):
            pos_frac = float(s.at[t, scol]) if scol in s.columns else 0.0
            pos_prev = prev_pos[scol]
            turnover = abs(pos_frac - pos_prev)
            capital = equity[f"short_{chr(96+idx)}"][t-1] if t>0 else start_cap  # short_a, short_b, short_c
            tc = turnover * (fee_bps / 1e4) * capital
            slippage_cost = 0.0
            if include_slippage and turnover > 0:
                notional_change = turnover * capital
                slippage_cost = slippage_model(notional_change, liq_t) * notional_change
            pnl = pos_frac * capital * (-ret_t) - tc - slippage_cost - pos_frac * capital * borrow_frac
            equity[f"short_{chr(96+idx)}"][t] = (equity[f"short_{chr(96+idx)}"][t-1] if t>0 else start_cap) + pnl
            prev_pos[scol] = pos_frac

        # Buy & hold benchmark
        equity["bh"][t] = (equity["bh"][t-1] if t>0 else start_cap) * np.exp(ret_t)

    equity_curves = pd.DataFrame({
        "long_s1": equity["long_s1"],
        "long_s2": equity["long_s2"],
        "long_s3": equity["long_s3"],
        "short_a": equity["short_a"],
        "short_b": equity["short_b"],
        "short_c": equity["short_c"],
        "bh": equity["bh"]
    }, index=dates)
    equity_curves.index.name = "date"

    # attach equity series to df for logging convenience
    d_out = d.copy()
    for col in equity_curves.columns:
        d_out[f"eq_{col}"] = equity_curves[col].values

    return d_out, equity_curves

# Compute PnL
bt_row, equity_curves = compute_pnl(long_bt, short_bt, start_cap=START_BALANCE)

# ---------------------------
# Decision log & forward crash realization
# ---------------------------
def build_decision_log(df_in: pd.DataFrame, H_horizon: int = int(H), crash_pct_local: float = float(crash_pct)) -> pd.DataFrame:
    dlog = df_in.copy().reset_index(drop=True)
    # ensure necessary columns exist
    cols = ["date", "price", "liquidity", "imbalance", "ret", "crash_prob", "ECDS",
            "position_s1", "position_s2", "position_s3"]
    for c in cols:
        if c not in dlog.columns:
            dlog[c] = np.nan
    # short flags
    for sname in ["short_A", "short_B", "short_C"]:
        if sname not in dlog.columns and sname in short_bt.columns:
            dlog[sname] = short_bt[sname].values
    # forward returns / crash flag
    dlog[f"fwd_return_{H_horizon}d"] = dlog["price"].shift(-H_horizon) / dlog["price"] - 1.0
    dlog[f"fwd_crash_{H_horizon}d"] = (dlog[f"fwd_return_{H_horizon}d"] <= -crash_pct_local).astype(int)
    return dlog

decision_log = build_decision_log(bt_row, H_horizon=int(H), crash_pct_local=float(crash_pct))
with st.expander("Decision log (exportable)"):
    st.dataframe(decision_log.tail(20), use_container_width=True)
    st.download_button("Download decision log CSV", data=decision_log.to_csv(index=False), file_name="decision_log.csv", mime="text/csv")

# ---------------------------
# Performance summary
# ---------------------------
def summarize_performance(eq: pd.DataFrame, ann_factor: int = 365) -> pd.DataFrame:
    rows = []
    for col in eq.columns:
        ser = eq[col].dropna()
        if len(ser) < 2:
            rows.append({"strategy": col, "cum_return": np.nan, "ann_vol": np.nan, "sharpe": np.nan, "max_drawdown": np.nan})
            continue
        daily_ret = ser.pct_change().fillna(0.0)
        cum_return = (ser.iloc[-1] / ser.iloc[0]) - 1.0
        ann_vol = daily_ret.std(ddof=0) * np.sqrt(ann_factor)
        ann_mean = daily_ret.mean() * ann_factor
        sharpe = (ann_mean / (ann_vol + 1e-12)) if ann_vol > 0 else np.nan
        roll_max = ser.cummax()
        drawdowns = (ser / roll_max) - 1.0
        max_dd = drawdowns.min()
        rows.append({"strategy": col, "cum_return": cum_return, "ann_vol": ann_vol, "sharpe": sharpe, "max_drawdown": max_dd})
    return pd.DataFrame(rows)

perf = summarize_performance(equity_curves)

st.subheader("Equity curves (per-strategy, $600 start)")
st.line_chart(equity_curves, use_container_width=True)

st.subheader("Performance summary")
st.dataframe(perf.style.format({
    "cum_return": "{:.2%}",
    "ann_vol": "{:.2f}",
    "sharpe": "{:.2f}",
    "max_drawdown": "{:.2%}"
}), use_container_width=True)

# Export equity curves
csv_eq = equity_curves.reset_index().to_csv(index=False)
st.download_button("Download equity curves CSV", data=csv_eq, file_name="equity_curves.csv", mime="text/csv")

st.info("Notes: This is a prototype research/backtest tool. Slippage, borrow costs and fills are modeled simply. Low-cap crypto is illiquid — treat simulated PnL with caution.")
