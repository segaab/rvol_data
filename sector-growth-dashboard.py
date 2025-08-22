import streamlit as st
import pandas as pd
import numpy as np
from yahooquery import Ticker
import plotly.graph_objects as go
from datetime import datetime, timedelta
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

LEADERS = {
    "Information Technology": {"AI": "NVDA", "Cloud Computing": "AMZN", "Cybersecurity": "CRWD"},
    "Healthcare": {"Telemedicine": "TDOC", "Biotechnology": "MRNA", "Medical Devices": "BSX", "Health Data Analytics": "TEM"},
    "Energy": {"Renewable Energy": "NEE", "Energy Storage": "STEM"},
    "Financials": {"Fintech": "SQ"},
    "Consumer Discretionary": {"E-commerce": "AMZN", "Electric Vehicles": "TSLA"},
    "Industrials": {"Robotics & Automation": "ISRG", "Aerospace Technology": "BA"},
    "Materials": {"Advanced Materials": "EMN", "Nanotechnology": "ZTEK", "Sustainable & Recycled Materials": "EMN"},
    "Utilities": {"Smart Grid Technology": "ITRI", "Renewable Electricity Integration": "PEG"},
    "Consumer Staples": {"Sustainable & Organic Food": "NSRGY", "Household & Personal Care": "PG"},
    "Communication Services": {"Social Media & Interactive Media": "META", "Telecom Services": "VZ"},
    "Real Estate": {"Proptech & Real Estate Tech": "Z", "REITs (Retail & Industrial)": "PLD"},
    "Basic Materials": {"Chemicals & Specialty Chemicals": "DOW", "Metals & Mining": "BHP"},
    "Oil & Gas": {"Major Integrated Oil Companies": "XOM", "Oilfield Services & Equipment": "SLB", "Canadian Oil & Gas": "MEG"},
    "Gold & Precious Metals": {"Gold Mining": "NEM", "Precious Metals Streaming & Royalty": "FNV"},
    "Arms/Defense": "LMT",
    "Cryptocurrency": "COIN"
}

# ==============================
# 2) Sidebar Controls
# ==============================
sector = st.sidebar.selectbox("Select Sector", list(SECTORS.keys()))
if isinstance(SECTORS[sector], dict):
    subsector_list = list(SECTORS[sector].keys())
    subsector = st.sidebar.selectbox("Select Niche/Subsector", subsector_list)
    tickers = SECTORS[sector][subsector]
    default_leader = LEADERS.get(sector, {}).get(subsector, tickers[0])
else:
    subsector = sector
    tickers = SECTORS[sector]
    default_leader = LEADERS.get(sector, tickers[0])

leader = st.sidebar.selectbox("Leader", tickers, index=tickers.index(default_leader))

# Default dates: latest - 1 year → latest
today = datetime.today().date()
default_end = today
default_start = today - timedelta(days=365)

start_date = st.sidebar.date_input("Start Date", value=default_start)
end_date = st.sidebar.date_input("End Date", value=default_end)

roc_window = st.sidebar.slider("ROC Window (days)", 5, 30, 14)

# ==============================
# 3) Data Fetching
# ==============================
@st.cache_data
def fetch_stock_data(symbols, start_date, end_date):
    t = Ticker(symbols)
    hist = t.history(start=start_date, end=end_date, interval="1d")
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index().pivot(index="date", columns="symbol", values="adjclose")
    return hist.fillna(method="ffill").dropna(how="all")

data = fetch_stock_data(tickers, start_date, end_date)

# ==============================
# Metrics, Phase Detection, Dot Plot (unchanged)
# ==============================
# ... (keep your existing metrics + charts code)

# ==============================
# 8) Expanded Download Metrics
# ==============================
# Add per-stock normalized return & progress
all_norm = {c: (data[c]/data[c].iloc[0] - 1)*100 for c in data.columns}
leader_norm = all_norm[leader]
avg_followers = pd.DataFrame(all_norm).drop(columns=leader).mean(axis=1)

leader_max = max(leader_norm.max(), 1e-9)
progress = pd.DataFrame({c: (all_norm[c]/leader_max).clip(0,1) for c in data.columns})
latest_progress = progress.iloc[-1]

metrics_df = pd.DataFrame({
    "Date": data.index,
    "Leader": leader_norm,
    "Followers_Avg": avg_followers
})
metrics_df["Negative_Space"] = leader_norm - avg_followers
metrics_df["Phase"] = "TODO: Phase logic here"  # (keep your existing phase logic)

# Merge all normalized returns
for stock in data.columns:
    metrics_df[f"{stock}_Return"] = all_norm[stock]
    metrics_df[f"{stock}_Progress"] = progress[stock]

st.download_button(
    "Download Full Metrics CSV",
    metrics_df.to_csv(index=False),
    file_name=f"{sector}_{subsector}_detailed_metrics.csv",
    mime="text/csv"
)
