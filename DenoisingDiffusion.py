import streamlit as st
import pandas as pd
import requests
import time
from datetime import datetime

# ==============================
# Streamlit Page Config
# ==============================
st.set_page_config(
    page_title="Small-Cap Crypto Dashboard",
    layout="wide"
)

st.title("📉 Small-Cap Crypto Dashboard with CoinGecko Pro API")

# ==============================
# API Setup
# ==============================
import os

COINGECKO_PRO_KEY = os.getenv("COINGECKO_PRO_KEY")  # Store your Pro API key in environment
if COINGECKO_PRO_KEY:
    COINGECKO_BASE = "https://pro-api.coingecko.com/api/v3"
    HEADERS = {"x-cg-pro-api-key": COINGECKO_PRO_KEY}
else:
    COINGECKO_BASE = "https://api.coingecko.com/api/v3"
    HEADERS = {}

# ==============================
# Helper Functions
# ==============================
def retry_request(url, params=None, max_retries=5, backoff=2):
    """
    Requests with retry/backoff for rate-limit errors (429)
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                st.warning(f"Rate limited. Retrying in {backoff} seconds...")
                time.sleep(backoff)
                backoff *= 2
            else:
                st.error(f"HTTPError: {e}")
                raise
        except Exception as e:
            st.error(f"Request failed: {e}")
            raise
    st.error("Max retries exceeded")
    return None

# Fetch all small-cap coins under a market cap threshold
@st.cache_data(show_spinner=True)
def fetch_smallcap_coins(vs_currency="usd", max_market_cap=50_000_000):
    url = f"{COINGECKO_BASE}/coins/markets"
    coins = []
    page = 1
    per_page = 250
    while True:
        params = {
            "vs_currency": vs_currency,
            "order": "market_cap_asc",
            "per_page": per_page,
            "page": page,
            "price_change_percentage": "24h"
        }
        data = retry_request(url, params)
        if not data:
            break
        for coin in data:
            if coin.get("market_cap", 0) <= max_market_cap:
                coins.append(coin)
        if len(data) < per_page:
            break
        page += 1
    return pd.DataFrame(coins)

# Fetch historical market chart data
@st.cache_data(show_spinner=True)
def fetch_market_chart(coin_id, vs_currency="usd", days=365):
    url = f"{COINGECKO_BASE}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days, "interval": "daily"}
    data = retry_request(url, params)
    if not data:
        return None
    df = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    df["date"] = pd.to_datetime(df["timestamp"], unit="ms")
    df.set_index("date", inplace=True)
    df.drop("timestamp", axis=1, inplace=True)
    return df

# ==============================
# Streamlit Sidebar
# ==============================
st.sidebar.header("Settings")
vs_currency = st.sidebar.selectbox("Select Currency", ["usd", "eur", "btc"], index=0)
market_cap_threshold = st.sidebar.number_input("Max Market Cap (USD)", value=50_000_000, step=1_000_000)
selected_coin = st.sidebar.text_input("Enter Coin ID (optional, e.g., 'akash')", "")

# ==============================
# Main App Logic
# ==============================
st.subheader("Fetching Small-Cap Coins...")
try:
    smallcap_df = fetch_smallcap_coins(vs_currency=vs_currency, max_market_cap=market_cap_threshold)
    st.success(f"Found {len(smallcap_df)} coins under ${market_cap_threshold:,}")
    st.dataframe(smallcap_df[["id", "symbol", "name", "market_cap", "current_price"]])
except Exception as e:
    st.error(f"Failed to load small-cap coins: {e}")

# Historical Data Section
if selected_coin:
    st.subheader(f"📊 Historical Price for {selected_coin}")
    try:
        hist_df = fetch_market_chart(selected_coin, vs_currency=vs_currency, days=365)
        if hist_df is not None and not hist_df.empty:
            st.line_chart(hist_df["price"])
        else:
            st.warning("No historical data found for this coin.")
    except Exception as e:
        st.error(f"Failed to load historical data: {e}")
