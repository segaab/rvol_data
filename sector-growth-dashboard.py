import streamlit as st
import pandas as pd
import numpy as np
from yahooquery import Ticker
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# -----------------------------
# Hide GitHub Icon
# -----------------------------
hide_github_icon = """
<style>
#GithubIcon {
    visibility: hidden;
}
</style>
"""
st.markdown(hide_github_icon, unsafe_allow_html=True)

# -----------------------------
# Streamlit page config
# -----------------------------
st.set_page_config(
    page_title="Sector Wave & Negative Space Dashboard",
    page_icon="📊",
    layout="wide"
)
st.title("📊 Sector Wave & Negative Space Dashboard")
st.markdown("**Sector Growth Strategy Backtest**")

# -----------------------------
# 1. Sector & Ticker Mapping
# -----------------------------
SECTORS = {
    "Information Technology": {
        "AI": ["Stock A", "Stock B", "Stock C", "Stock D"],
        "Cloud Computing": ["Stock E", "Stock F", "Stock G", "Stock H"],
        "Cybersecurity": ["Stock I", "Stock J", "Stock K", "Stock L"]
    },
    "Healthcare": {
        "Telemedicine": ["Stock M", "Stock N", "Stock O", "Stock P"],
        "Biotechnology": ["Stock Q", "Stock R", "Stock S", "Stock T"],
        "Medical Devices": ["Stock U", "Stock V", "Stock W", "Stock X"]
    },
    "Energy": {
        "Renewable Energy": ["Stock Y", "Stock Z", "Stock AA", "Stock AB"],
        "Energy Storage": ["Stock AC", "Stock AD", "Stock AE", "Stock AF"]
    },
    "Financials": {
        "Fintech": ["Stock AG", "Stock AH", "Stock AI", "Stock AJ"]
    },
    "Consumer Discretionary": {
        "E-commerce": ["Stock AK", "Stock AL", "Stock AM", "Stock AN"],
        "Electric Vehicles": ["Stock AO", "Stock AP", "Stock AQ", "Stock AR"]
    },
    "Industrials": {
        "Robotics & Automation": ["Stock AS", "Stock AT", "Stock AU", "Stock AV"],
        "Aerospace Technology": ["Stock AW", "Stock AX", "Stock AY", "Stock AZ"]
    },
    "Materials": {
        "Advanced Materials": ["Stock BA", "Stock BB", "Stock BC", "Stock BD"],
        "Nanotechnology": ["Stock BE", "Stock BF", "Stock BG", "Stock BH"],
        "Sustainable & Recycled Materials": ["Stock BI", "Stock BJ", "Stock BK", "Stock BL"]
    },
    "Utilities": {
        "Smart Grid Technology": ["Stock BM", "Stock BN", "Stock BO", "Stock BP"],
        "Renewable Electricity Integration": ["Stock BQ", "Stock BR", "Stock BS", "Stock BT"]
    },
    "Consumer Staples": {
        "Sustainable & Organic Food": ["Stock BU", "Stock BV", "Stock BW", "Stock BX"],
        "Household & Personal Care": ["Stock BY", "Stock BZ", "Stock CA", "Stock CB"]
    },
    "Communication Services": {
        "Social Media & Interactive Media": ["Stock CC", "Stock CD", "Stock CE", "Stock CF"],
        "Telecom Services": ["Stock CG", "Stock CH", "Stock CI", "Stock CJ"]
    },
    "Real Estate": {
        "Proptech & Real Estate Tech": ["Stock CK", "Stock CL", "Stock CM", "Stock CN"],
        "REITs (Retail & Industrial)": ["Stock CO", "Stock CP", "Stock CQ", "Stock CR"]
    },
    "Basic Materials": {
        "Chemicals & Specialty Chemicals": ["Stock CS", "Stock CT", "Stock CU", "Stock CV"],
        "Metals & Mining": ["Stock CW", "Stock CX", "Stock CY", "Stock CZ"]
    },
    "Oil & Gas": {
        "Major Integrated Oil Companies": ["Stock DA", "Stock DB", "Stock DC", "Stock DD"],
        "Oilfield Services & Equipment": ["Stock DE", "Stock DF", "Stock DG", "Stock DH"],
        "Canadian Oil & Gas": ["Stock DI", "Stock DJ", "Stock DK", "Stock DL"]
    },
    "Gold & Precious Metals": {
        "Gold Mining": ["Stock DM", "Stock DN", "Stock DO", "Stock DP"],
        "Precious Metals Streaming & Royalty": ["Stock DQ", "Stock DR", "Stock DS", "Stock DT"]
    },
    "Arms/Defense": ["Stock DU", "Stock DV", "Stock DW", "Stock DX"],
    "Cryptocurrency": ["Stock DY", "Stock DZ", "Stock EA", "Stock EB"]
}

# -----------------------------
# 2. Sidebar
# -----------------------------
sector = st.sidebar.selectbox("Select Sector", list(SECTORS.keys()))
subsector_list = list(SECTORS[sector].keys()) if isinstance(SECTORS[sector], dict) else [sector]
subsector = st.sidebar.selectbox("Select Niche/Subsector", subsector_list)
tickers = SECTORS[sector][subsector] if isinstance(SECTORS[sector], dict) else SECTORS[sector]
leader = st.sidebar.selectbox("Select Leader", tickers)

# Default dates: end_date = today, start_date = 1 year before
today = datetime.today()
default_start = today - timedelta(days=365)
start_date = st.sidebar.date_input("Start Date", value=default_start)
end_date = st.sidebar.date_input("End Date", value=today)

# Fixed metrics defaults
roc_window = 14
neg_space_threshold = 0.05

# -----------------------------
# 3. Data Fetching
# -----------------------------
@st.cache_data
def fetch_stock_data(symbols, start_date, end_date):
    ticker = Ticker(symbols)
    data = ticker.history(start=start_date, end=end_date, interval='1d')
    if isinstance(data.index, pd.MultiIndex):
        data = data.reset_index().pivot(index="date", columns="symbol", values="adjclose")
    return data.fillna(method='ffill').dropna()

data = fetch_stock_data(tickers, start_date, end_date)

# -----------------------------
# 4. Metrics & Phase Detection
# -----------------------------
def normalize_prices(prices):
    return (prices / prices.iloc[0] - 1) * 100

def calculate_negative_space(leader_prices, follower_prices):
    leader_norm = normalize_prices(leader_prices)
    follower_norms = pd.concat([normalize_prices(follower_prices[c]) for c in follower_prices.columns], axis=1)
    avg_follower_norm = follower_norms.mean(axis=1)
    negative_space = leader_norm - avg_follower_norm
    return negative_space, leader_norm, avg_follower_norm

def calculate_roc(series, window):
    return series.pct_change(window) * 100

def identify_phases(negative_space, roc_neg_space, acc_neg_space):
    phases = []
    for i in range(len(negative_space)):
        if pd.isna(roc_neg_space.iloc[i]) or pd.isna(acc_neg_space.iloc[i]):
            phases.append("Neutral")
        elif roc_neg_space.iloc[i] > 0 and negative_space.iloc[i] > 0:
            phases.append("Early Stage")
        elif roc_neg_space.iloc[i] < 0 and acc_neg_space.iloc[i] < 0:
            phases.append("Building Stage")
        elif roc_neg_space.iloc[i] < 0 and acc_neg_space.iloc[i] >= 0:
            phases.append("Expansion")
        elif roc_neg_space.iloc[i] >= 0 and negative_space.iloc[i] < 0:
            phases.append("Maturity")
        elif roc_neg_space.iloc[i] > 0 and negative_space.iloc[i] > 0:
            phases.append("Pullback")
        else:
            phases.append("Neutral")
    return phases

leader_prices = data[leader]
follower_prices = data[[c for c in tickers if c != leader]]
negative_space, leader_norm, avg_follower_norm = calculate_negative_space(leader_prices, follower_prices)
roc_neg_space = calculate_roc(negative_space, roc_window)
acc_neg_space = calculate_roc(roc_neg_space, roc_window)
phases = identify_phases(negative_space, roc_neg_space, acc_neg_space)

# -----------------------------
# 5. Main Display
# -----------------------------
st.subheader("Normalized Growth & Metrics")
st.line_chart(pd.concat([leader_norm, avg_follower_norm], axis=1))

st.subheader("Negative Space & ROC")
fig_neg = go.Figure()
fig_neg.add_trace(go.Scatter(x=data.index, y=negative_space, name='Negative Space', line=dict(color='red')))
fig_neg.add_trace(go.Scatter(x=data.index, y=roc_neg_space, name='ROC', line=dict(color='purple')))
st.plotly_chart(fig_neg, use_container_width=True)

st.subheader("Current Phase")
st.markdown(f"**{phases[-1]}**")

# -----------------------------
# 6. Dot Plot: Progression Toward Leader
# -----------------------------
st.subheader("Stock Progression Toward Sector Inflection")
progress = pd.DataFrame({f"Stock {chr(65+i)}": normalize_prices(data[c])/normalize_prices(data[leader]).max()
                         for i, c in enumerate(tickers)})
latest_progress = progress.iloc[-1]

fig_dot = go.Figure()
for stock in progress.columns:
    fig_dot.add_trace(go.Scatter(
        x=[stock],
        y=[latest_progress[stock]],
        mode='markers+lines',
        name=stock,
        line=dict(width=2),
        marker=dict(size=12, color='blue'),
        text=[f"Phase: {phases[-1]}<br>Progress: {latest_progress[stock]:.2f}"],
        hoverinfo="text"
    ))

fig_dot.update_layout(
    yaxis_title="Progress to Sector Inflection",
    xaxis_title="Stocks",
    yaxis=dict(range=[0, 1]),
    height=500
)
st.plotly_chart(fig_dot, use_container_width=True)

# -----------------------------
# 7. Download Metrics
# -----------------------------
metrics_df = pd.DataFrame({
    "Date": data.index,
    "Leader": leader_norm,
    "Followers_Avg": avg_follower_norm,
    "Negative_Space": negative_space,
    "ROC": roc_neg_space,
    "ACC_Negative_Space": acc_neg_space,
    "Phase": phases
})
st.download_button("Download Metrics CSV", metrics_df.to_csv(index=False), file_name=f"{sector}_{subsector}_metrics.csv")
