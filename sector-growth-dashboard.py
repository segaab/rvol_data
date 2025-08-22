
    mime="text/csv"
)
import streamlit as st
import pandas as pd
import numpy as np
from yahooquery import Ticker
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

# ==============================
# Streamlit Page Config
# ==============================
st.set_page_config(
    page_title="Sector Wave & Negative Space Dashboard",
    page_icon="📊",
    layout="wide"
)
st.title("📊 Sector Wave & Negative Space Dashboard")

# ==============================
# 1) Sector, Ticker & Leader Mapping
# ==============================
SECTORS = {
    "Information Technology": {
        "AI": ["NVDA", "AMD", "PLTR", "SMCI", "SNOW", "AVGO"],
        "Cloud Computing": ["AMZN", "MSFT", "NET", "ORCL", "SMCI", "PLTR"],
        "Cybersecurity": ["CRWD", "ZS", "FTNT", "CYBR", "FFIV", "AVGO"]
    },
    "Healthcare": {
        "Telemedicine": ["TDOC", "DOCS", "AMWL", "PINC", "PHR", "IRTC"],
        "Biotechnology": ["MRNA", "REGN", "VRTX", "BTAI", "TNXP", "SNGX"],
        "Medical Devices": ["BSX", "MDT", "ABT", "SNN", "ZBH", "MASI"],
        "Health Data Analytics": ["TEM", "GEHC", "MDT", "BSX", "BDX", "GH"]
    },
    "Energy": {
        "Renewable Energy": ["NEE", "FSLR", "BEP", "ENLT", "RNW", "CEG"],
        "Energy Storage": ["STEM", "BE", "AEE", "CMS", "CEG", "ENPH"]
    },
    "Financials": {
        "Fintech": ["SQ", "SOFI", "PYPL", "TW", "HOOD", "IBKR"]
    },
    "Consumer Discretionary": {
        "E-commerce": ["AMZN", "SHOP", "WMT", "PYPL", "CVNA", "GRPN"],
        "Electric Vehicles": ["TSLA", "BYDDY", "NIO", "VWAGY", "RIVN", "LCID"]
    },
    "Industrials": {
        "Robotics & Automation": ["ISRG", "TXN", "ABB", "TER", "SYM", "PATH"],
        "Aerospace Technology": ["BA", "LMT", "RTX", "GD", "TDY", "HII"]
    },
    "Materials": {
        "Advanced Materials": ["EMN", "HTGC", "KMT", "RYAM", "HUN", "ASIX"],
        "Nanotechnology": ["ZTEK", "NNOMF", "NNXPF", "ATOM", "AVAV", "ONTO"],
        "Sustainable & Recycled Materials": ["EMN", "ECL", "SMG", "PKG", "WRK", "SEE"]
    },
    "Utilities": {
        "Smart Grid Technology": ["ITRI", "GRID", "IPWR", "BMI", "HASI", "ARQ"],
        "Renewable Electricity Integration": ["PEG", "NEE", "ED", "SO", "DUK", "ES"]
    },
    "Consumer Staples": {
        "Sustainable & Organic Food": ["NSRGY", "TSN", "CLX", "MO", "OLLI", "COCO"],
        "Household & Personal Care": ["PG", "CLX", "KMB", "UL", "CL", "EL"]
    },
    "Communication Services": {
        "Social Media & Interactive Media": ["META", "SNAP", "PINS", "SPOT", "BILI", "TWTR"],
        "Telecom Services": ["VZ", "T", "TMUS", "CMCSA", "DISH", "NOVN"]
    },
    "Real Estate": {
        "Proptech & Real Estate Tech": ["Z", "OPEN", "RDFN", "AURUMPROP", "COMP", "DOC"],
        "REITs (Retail & Industrial)": ["PLD", "AMT", "SPG", "PSA", "EQR", "DLR"]
    },
    "Basic Materials": {
        "Chemicals & Specialty Chemicals": ["DOW", "LYB", "SHW", "HUN", "ECL", "PPG"],
        "Metals & Mining": ["BHP", "RIO", "FCX", "NEM", "GOLD", "VALE"]
    },
    "Oil & Gas": {
        "Major Integrated Oil Companies": ["XOM", "CVX", "COP", "SLB", "WMB", "EOG"],
        "Oilfield Services & Equipment": ["SLB", "HAL", "BKR", "NOV", "FTI", "COS"],
        "Canadian Oil & Gas": ["MEG", "FO", "ATH", "HWX", "CNQ", "SU"]
    },
    "Gold & Precious Metals": {
        "Gold Mining": ["NEM", "GOLD", "KL", "AEM", "FNV", "ABX"],
        "Precious Metals Streaming & Royalty": ["FNV", "WPM", "RGLD", "HL", "PAAS", "AUY"]
    },
    "Arms/Defense": ["LMT", "RTX", "BA", "HON", "GD", "NOC"],
    "Cryptocurrency": ["COIN", "MSTR", "RIOT", "MARA", "BLOK", "CORE", "CLSK", "HUT", "HOOD", "TERA"]
}

# Leader mapping: sector -> subsector -> leader (or sector -> leader for list-only sectors)
LEADERS = {
    "Information Technology": {
        "AI": "NVDA",
        "Cloud Computing": "AMZN",
        "Cybersecurity": "CRWD"
    },
    "Healthcare": {
        "Telemedicine": "TDOC",
        "Biotechnology": "MRNA",
        "Medical Devices": "BSX",
        "Health Data Analytics": "TEM"
    },
    "Energy": {
        "Renewable Energy": "NEE",
        "Energy Storage": "STEM"
    },
    "Financials": {
        "Fintech": "SQ"
    },
    "Consumer Discretionary": {
        "E-commerce": "AMZN",
        "Electric Vehicles": "TSLA"
    },
    "Industrials": {
        "Robotics & Automation": "ISRG",
        "Aerospace Technology": "BA"
    },
    "Materials": {
        "Advanced Materials": "EMN",
        "Nanotechnology": "ZTEK",
        "Sustainable & Recycled Materials": "EMN"
    },
    "Utilities": {
        "Smart Grid Technology": "ITRI",
        "Renewable Electricity Integration": "PEG"
    },
    "Consumer Staples": {
        "Sustainable & Organic Food": "NSRGY",
        "Household & Personal Care": "PG"
    },
    "Communication Services": {
        "Social Media & Interactive Media": "META",
        "Telecom Services": "VZ"
    },
    "Real Estate": {
        "Proptech & Real Estate Tech": "Z",
        "REITs (Retail & Industrial)": "PLD"
    },
    "Basic Materials": {
        "Chemicals & Specialty Chemicals": "DOW",
        "Metals & Mining": "BHP"
    },
    "Oil & Gas": {
        "Major Integrated Oil Companies": "XOM",
        "Oilfield Services & Equipment": "SLB",
        "Canadian Oil & Gas": "MEG"
    },
    "Gold & Precious Metals": {
        "Gold Mining": "NEM",
        "Precious Metals Streaming & Royalty": "FNV"
    },
    "Arms/Defense": "LMT",
    "Cryptocurrency": "COIN"
}

# ==============================
# 2) Sidebar Controls
# ==============================
sector = st.sidebar.selectbox("Select Sector", list(SECTORS.keys()))

# Handle subsector presence/absence
if isinstance(SECTORS[sector], dict):
    subsector_list = list(SECTORS[sector].keys())
    subsector = st.sidebar.selectbox("Select Niche/Subsector", subsector_list)
    tickers = SECTORS[sector][subsector]
    default_leader = LEADERS.get(sector, {}).get(subsector, tickers[0])
else:
    subsector = sector  # flat sector
    tickers = SECTORS[sector]
    default_leader = LEADERS.get(sector, tickers[0])

st.sidebar.caption(
    f"**Default Leader:** {default_leader}  \n"
    f"(*From the curated leader mapping for {subsector}*)"
)

# Allow override but default to mapped leader
leader = st.sidebar.selectbox("Leader (override optional)", tickers, index=tickers.index(default_leader))

start_date = st.sidebar.date_input("Start Date", value=datetime(2023, 1, 1))
end_date = st.sidebar.date_input("End Date", value=datetime(2024, 7, 1))

roc_window = st.sidebar.slider("ROC Window (days)", min_value=5, max_value=30, value=14)
neg_space_threshold = st.sidebar.slider("Negative Space Threshold", min_value=0.01, max_value=0.10, value=0.05, step=0.01)

# ==============================
# 3) Data Fetching
# ==============================
@st.cache_data
def fetch_stock_data(symbols, start_date, end_date):
    t = Ticker(symbols)
    hist = t.history(start=start_date, end=end_date, interval="1d")
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index().pivot(index="date", columns="symbol", values="adjclose")
    # ffill then drop leading NaNs
    return hist.fillna(method="ffill").dropna(how="all")

data = fetch_stock_data(tickers, start_date, end_date)
if data is None or data.empty:
    st.stop()

# ==============================
# 4) Metrics & Phase Detection
# ==============================
def normalize_prices(prices: pd.Series) -> pd.Series:
    return (prices / prices.iloc[0] - 1.0) * 100.0

def calc_neg_space(leader_prices: pd.Series, follower_prices: pd.DataFrame):
    leader_norm = normalize_prices(leader_prices)
    follower_norms = pd.concat([normalize_prices(follower_prices[c]) for c in follower_prices.columns], axis=1)
    avg_follower_norm = follower_norms.mean(axis=1)
    neg_space = leader_norm - avg_follower_norm
    return neg_space, leader_norm, avg_follower_norm

def roc(series: pd.Series, win: int) -> pd.Series:
    return series.pct_change(win) * 100.0

def identify_phases(neg_space: pd.Series, roc_ns: pd.Series, acc_ns: pd.Series):
    """
    Generic, non-proprietary phase rules:
    - Inactive: insufficient data or flat dynamics
    - Initiation: neg space > 0 and increasing
    - Early Inflection: neg space shrinking and acceleration negative
    - Mid Inflection: neg space shrinking but acceleration >= 0
    - Late Inflection: neg space <= 0 (followers ~ caught up) with non-negative ROC
    - Interruption: neg space > 0 and ROC > 0 after inflection (resumed divergence)
    """
    phases = []
    for i in range(len(neg_space)):
        ns = neg_space.iloc[i]
        r = roc_ns.iloc[i] if not pd.isna(roc_ns.iloc[i]) else np.nan
        a = acc_ns.iloc[i] if not pd.isna(acc_ns.iloc[i]) else np.nan

        if np.isnan(r) or np.isnan(a):
            phases.append("Inactive")
            continue

        if r > 0 and ns > 0:
            phases.append("Initiation")
        elif r < 0 and a < 0:
            phases.append("Early Inflection")
        elif r < 0 and a >= 0:
            phases.append("Mid Inflection")
        elif r >= 0 and ns <= 0:
            phases.append("Late Inflection")
        elif r > 0 and ns > 0:
            phases.append("Interruption")
        else:
            phases.append("Inactive")
    return phases

leader_prices = data[leader]
followers = [s for s in data.columns if s != leader]
follower_prices = data[followers] if followers else pd.DataFrame(index=data.index)

neg_space, leader_norm, avg_follower_norm = calc_neg_space(leader_prices, follower_prices) if not follower_prices.empty else (pd.Series(0, index=data.index), normalize_prices(leader_prices), pd.Series(0, index=data.index))
roc_neg_space = roc(neg_space, roc_window)
acc_neg_space = roc(roc_neg_space, roc_window)
phases = identify_phases(neg_space, roc_neg_space, acc_neg_space)

# ==============================
# 5) Main Display (Charts)
# ==============================
st.subheader("Normalized Prices (Leader vs Followers Avg)")

norm_df = pd.DataFrame({
    f"{leader} (Leader)": leader_norm,
    "Followers Avg": avg_follower_norm
})
st.line_chart(norm_df)

st.subheader("Negative Space & ROC")
fig_neg = go.Figure()
fig_neg.add_trace(go.Scatter(x=data.index, y=neg_space, name="Negative Space", line=dict(width=2)))
fig_neg.add_trace(go.Scatter(x=data.index, y=roc_neg_space, name="ROC (Negative Space)", line=dict(width=2)))
fig_neg.add_hline(y=0, line_dash="dot", line_color="gray")
fig_neg.update_layout(height=420, legend_orientation="h", margin=dict(l=10, r=10, t=30, b=10))
st.plotly_chart(fig_neg, use_container_width=True)

# Phase badge
st.subheader("Current Phase")
st.markdown(f"**{phases[-1]}**")

# ==============================
# 6) Dot Plot: Vertical Progress Toward Inflection
# ==============================
st.subheader("Vertical Dot Plot: Progress Toward Inflection (0 → 1)")

# Progress proxy: each stock's normalized return divided by leader's max normalized return
leader_max = max(leader_norm.max(), 1e-9)
progress = pd.DataFrame({c: (normalize_prices(data[c]) / leader_max).clip(lower=0, upper=1) for c in data.columns})
latest_progress = progress.iloc[-1]

fig_dot = go.Figure()

# Horizontal guide lines for visual "phase rails" (non-proprietary visual cues)
guides = [
    (0.15, "Initiation zone"),
    (0.40, "Early Inflection zone"),
    (0.65, "Mid Inflection zone"),
    (0.85, "Late Inflection zone")
]
for y, label in guides:
    fig_dot.add_hline(y=y, line_dash="dot", opacity=0.4)
    fig_dot.add_annotation(xref="paper", x=1.0, y=y, text=label, showarrow=False, xanchor="right", yanchor="bottom", font=dict(size=10))

# Plot each stock as a point at its latest progress (y), x=categorical stock name
for stock in data.columns:
    is_leader = (stock == leader)
    fig_dot.add_trace(
        go.Scatter(
            x=[stock],
            y=[latest_progress[stock]],
            mode="markers",
            marker=dict(size=14, symbol="circle", line=dict(width=1)),
            name=f"{stock}{' (Leader)' if is_leader else ''}",
            hovertemplate=f"<b>{stock}</b>"
                          f"<br>Progress: {latest_progress[stock]:.2f}"
                          f"<br>Phase: {phases[-1]}"
                          "<extra></extra>"
        )
    )

fig_dot.update_layout(
    yaxis_title="Progress to Inflection (visual proxy)",
    xaxis_title="Constituents",
    yaxis=dict(range=[0, 1]),
    height=520,
    margin=dict(l=10, r=10, t=30, b=10),
    showlegend=True,
)
st.plotly_chart(fig_dot, use_container_width=True)

# ==============================
# 7) Constituents & Leader Note
# ==============================
with st.expander("Constituents & Leader Mapping"):
    if isinstance(SECTORS[sector], dict):
        st.write(f"**{sector} → {subsector}** constituents:")
        st.write(", ".join(tickers))
        st.write(f"**Mapped Leader:** {default_leader}")
    else:
        st.write(f"**{sector}** constituents:")
        st.write(", ".join(tickers))
        st.write(f"**Mapped Leader:** {default_leader}")

# ==============================
# 8) Download Metrics
# ==============================
metrics_df = pd.DataFrame({
    "Date": data.index,
    "Leader": leader_norm,
    "Followers_Avg": avg_follower_norm,
    "Negative_Space": neg_space,
    "ROC_Negative_Space": roc_neg_space,
    "ACC_Negative_Space": acc_neg_space,
    "Phase": phases
})
st.download_button(
    "Download Metrics CSV",
    metrics_df.to_csv(index=False),
    file_name=f"{sector}_{subsector}_metrics.csv",
    mime="text/csv"
)
