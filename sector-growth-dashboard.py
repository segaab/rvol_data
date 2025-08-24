# ==============================
# Imports & Setup
# ==============================
import os
import json
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client
from yahooquery import Ticker

warnings.filterwarnings("ignore")
load_dotenv()

# ==============================
# Streamlit Page Config
# ==============================
st.set_page_config(
    page_title="Sector Wave & Negative Space Dashboard",
    page_icon="📊",
    layout="wide",
)
st.title("📊 Sector Wave & Negative Space Dashboard")

# ==============================
# Supabase Configuration
# ==============================
@st.cache_resource
def init_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        st.error(
            "Supabase credentials not found. "
            "Please set SUPABASE_URL and SUPABASE_KEY in your .env file."
        )
        st.stop()
    return create_client(url, key)

supabase = init_supabase()

# ==============================
# 1) Sector, Ticker & Leader Mapping
# ==============================
SECTORS = {
    "Information Technology": {
        "AI": ["NVDA", "AMD", "PLTR", "SMCI", "SNOW", "AVGO"],
        "Cloud Computing": ["AMZN", "MSFT", "NET", "ORCL", "SMCI", "PLTR"],
        "Cybersecurity": ["CRWD", "ZS", "FTNT", "CYBR", "FFIV", "AVGO"],
    },
    "Healthcare": {
        "Telemedicine": ["TDOC", "DOCS", "AMWL", "PINC", "PHR", "IRTC"],
        "Biotechnology": ["MRNA", "REGN", "VRTX", "BTAI", "TNXP", "SNGX"],
        "Medical Devices": ["BSX", "MDT", "ABT", "SNN", "ZBH", "MASI"],
        "Health Data Analytics": ["TEM", "GEHC", "MDT", "BSX", "BDX", "GH"],
    },
    "Energy": {
        "Renewable Energy": ["NEE", "FSLR", "BEP", "ENLT", "RNW", "CEG"],
        "Energy Storage": ["STEM", "BE", "AEE", "CMS", "CEG", "ENPH"],
    },
    "Financials": {"Fintech": ["SQ", "SOFI", "PYPL", "TW", "HOOD", "IBKR"]},
    "Consumer Discretionary": {
        "E-commerce": ["AMZN", "SHOP", "WMT", "PYPL", "CVNA", "GRPN"],
        "Electric Vehicles": ["TSLA", "BYDDY", "NIO", "VWAGY", "RIVN", "LCID"],
    },
    "Industrials": {
        "Robotics & Automation": ["ISRG", "TXN", "ABB", "TER", "SYM", "PATH"],
        "Aerospace Technology": ["BA", "LMT", "RTX", "GD", "TDY", "HII"],
    },
    "Materials": {
        "Advanced Materials": ["EMN", "HTGC", "KMT", "RYAM", "HUN", "ASIX"],
        "Nanotechnology": ["ZTEK", "NNOMF", "NNXPF", "ATOM", "AVAV", "ONTO"],
        "Sustainable & Recycled Materials": ["EMN", "ECL", "SMG", "PKG", "WRK", "SEE"],
    },
    "Utilities": {
        "Smart Grid Technology": ["ITRI", "GRID", "IPWR", "BMI", "HASI", "ARQ"],
        "Renewable Electricity Integration": ["PEG", "NEE", "ED", "SO", "DUK", "ES"],
    },
    "Consumer Staples": {
        "Sustainable & Organic Food": ["NSRGY", "TSN", "CLX", "MO", "OLLI", "COCO"],
        "Household & Personal Care": ["PG", "CLX", "KMB", "UL", "CL", "EL"],
    },
    "Communication Services": {
        "Social Media & Interactive Media": ["META", "SNAP", "PINS", "SPOT", "BILI", "TWTR"],
        "Telecom Services": ["VZ", "T", "TMUS", "CMCSA", "DISH", "NOVN"],
    },
    "Real Estate": {
        "Proptech & Real Estate Tech": ["Z", "OPEN", "RDFN", "AURUMPROP", "COMP", "DOC"],
        "REITs (Retail & Industrial)": ["PLD", "AMT", "SPG", "PSA", "EQR", "DLR"],
    },
    "Basic Materials": {
        "Chemicals & Specialty Chemicals": ["DOW", "LYB", "SHW", "HUN", "ECL", "PPG"],
        "Metals & Mining": ["BHP", "RIO", "FCX", "NEM", "GOLD", "VALE"],
    },
    "Oil & Gas": {
        "Major Integrated Oil Companies": ["XOM", "CVX", "COP", "SLB", "WMB", "EOG"],
        "Oilfield Services & Equipment": ["SLB", "HAL", "BKR", "NOV", "FTI", "COS"],
        "Canadian Oil & Gas": ["MEG", "FO", "ATH", "HWX", "CNQ", "SU"],
    },
    "Gold & Precious Metals": {
        "Gold Mining": ["NEM", "GOLD", "KL", "AEM", "FNV", "ABX"],
        "Precious Metals Streaming & Royalty": ["FNV", "WPM", "RGLD", "HL", "PAAS", "AUY"],
    },
    "Arms/Defense": ["LMT", "RTX", "BA", "HON", "GD", "NOC"],
    "Cryptocurrency": ["COIN", "MSTR", "RIOT", "MARA", "BLOK", "CORE", "CLSK", "HUT", "HOOD", "TERA"],
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
    "Cryptocurrency": "COIN",
}

# ==============================
# 2) Helpers: Metrics & Phases
# ==============================
def normalize_prices(prices: pd.Series) -> pd.Series:
    if prices.isna().all():
        return pd.Series(index=prices.index, dtype=float)
    first = prices.dropna().iloc[0]
    if first == 0:
        return pd.Series(index=prices.index, dtype=float)
    return (prices / first - 1.0) * 100.0

def calc_neg_space(leader_prices: pd.Series, follower_prices: pd.DataFrame):
    leader_norm = normalize_prices(leader_prices)
    if follower_prices is None or follower_prices.empty:
        avg_follower_norm = pd.Series(0, index=leader_norm.index)
    else:
        follower_norms = pd.concat(
            [normalize_prices(follower_prices[c]) for c in follower_prices.columns],
            axis=1,
        )
        avg_follower_norm = follower_norms.mean(axis=1)
    neg_space = leader_norm - avg_follower_norm
    return neg_space, leader_norm, avg_follower_norm

def roc(series: pd.Series, win: int) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(dtype=float)
    return series.pct_change(win) * 100.0

def identify_phases(neg_space: pd.Series, roc_ns: pd.Series, acc_ns: pd.Series):
    """
    Generic, non-proprietary phase rules:
    - Inactive: insufficient data or flat dynamics
    - Initiation: neg space > 0 and ROC > 0
    - Early Inflection: ROC < 0 and ACC < 0
    - Mid Inflection: ROC < 0 and ACC >= 0
    - Late Inflection: neg space <= 0 and ROC >= 0
    - Interruption: neg space > 0 and ROC > 0 after inflection
    """
    phases = []
    for i in range(len(neg_space)):
        ns = neg_space.iloc[i]
        r = roc_ns.iloc[i] if i < len(roc_ns) else np.nan
        a = acc_ns.iloc[i] if i < len(acc_ns) else np.nan
        if np.isnan(r) or np.isnan(a):
            phases.append("Inactive")
        elif r > 0 and ns > 0:
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

# ==============================
# 3) Database Operations
# ==============================
@st.cache_data(ttl="1h")
def get_sector_id(sector_name: str):
    res = supabase.table("sectors").select("id").eq("name", sector_name).execute()
    if res.data:
        return res.data[0]["id"]
    ins = supabase.table("sectors").insert({"name": sector_name}).execute()
    return ins.data[0]["id"]

@st.cache_data(ttl="1h")
def get_subsector_id(subsector_name: str, sector_id: int):
    res = (
        supabase.table("subsectors")
        .select("id")
        .eq("name", subsector_name)
        .eq("sector_id", sector_id)
        .execute()
    )
    if res.data:
        return res.data[0]["id"]
    ins = supabase.table("subsectors").insert({"name": subsector_name, "sector_id": sector_id}).execute()
    return ins.data[0]["id"]

@st.cache_data(ttl="1h")
def get_ticker_id(ticker_symbol: str):
    res = supabase.table("tickers").select("id").eq("symbol", ticker_symbol).execute()
    if res.data:
        return res.data[0]["id"]
    ins = supabase.table("tickers").insert({"symbol": ticker_symbol}).execute()
    return ins.data[0]["id"]

def ensure_subsector_tickers(subsector_id: int, ticker_ids: list[int], leader_id: int):
    for tid in ticker_ids:
        res = (
            supabase.table("subsector_tickers")
            .select("id,is_leader")
            .eq("subsector_id", subsector_id)
            .eq("ticker_id", tid)
            .execute()
        )
        if not res.data:
            supabase.table("subsector_tickers").insert(
                {"subsector_id": subsector_id, "ticker_id": tid, "is_leader": tid == leader_id}
            ).execute()
        else:
            mapping_id = res.data[0]["id"]
            supabase.table("subsector_tickers").update(
                {"is_leader": tid == leader_id}
            ).eq("id", mapping_id).execute()

def save_price_data(ticker_ids: list[int], symbols: list[str], data: pd.DataFrame):
    for symbol, ticker_id in zip(symbols, ticker_ids):
        if symbol not in data.columns:
            continue
        prices = data[symbol].dropna()
        if prices.empty:
            continue
        records = []
        for date, price in prices.items():
            ds = pd.to_datetime(date).strftime("%Y-%m-%d")
            exists = (
                supabase.table("price_data")
                .select("id")
                .eq("ticker_id", ticker_id)
                .eq("date", ds)
                .execute()
            )
            if not exists.data:
                records.append({"ticker_id": ticker_id, "date": ds, "adj_close": float(price)})
        for i in range(0, len(records), 100):
            batch = records[i : i + 100]
            if batch:
                supabase.table("price_data").insert(batch).execute()

def save_sector_metrics(sector_id: int, subsector_id: int, leader_id: int, metrics_df: pd.DataFrame, roc_window: int):
    records = []
    for idx, row in metrics_df.iterrows():
        ds = pd.to_datetime(idx).strftime("%Y-%m-%d")
        exists = (
            supabase.table("sector_metrics")
            .select("id")
            .eq("subsector_id", subsector_id)
            .eq("leader_id", leader_id)
            .eq("date", ds)
            .eq("roc_window", roc_window)
            .execute()
        )
        if not exists.data:
            records.append(
                {
                    "sector_id": sector_id,
                    "subsector_id": subsector_id,
                    "leader_id": leader_id,
                    "date": ds,
                    "roc_window": roc_window,
                    "leader_norm": float(row.get("Leader", np.nan)) if pd.notna(row.get("Leader", np.nan)) else None,
                    "followers_avg_norm": float(row.get("Followers_Avg", np.nan)) if pd.notna(row.get("Followers_Avg", np.nan)) else None,
                    "negative_space": float(row.get("Negative_Space", np.nan)) if pd.notna(row.get("Negative_Space", np.nan)) else None,
                    "roc_neg_space": float(row.get("ROC_Negative_Space", np.nan)) if pd.notna(row.get("ROC_Negative_Space", np.nan)) else None,
                    "acc_neg_space": float(row.get("ACC_Negative_Space", np.nan)) if pd.notna(row.get("ACC_Negative_Space", np.nan)) else None,
                    "phase": row.get("Phase", None),
                }
            )
    for i in range(0, len(records), 100):
        batch = records[i : i + 100]
        if batch:
            supabase.table("sector_metrics").insert(batch).execute()

@st.cache_data(ttl="1d")
def get_stored_metrics(subsector_id: int, leader_id: int, roc_window: int, start_date: datetime, end_date: datetime):
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    res = (
        supabase.table("sector_metrics")
        .select("*")
        .eq("subsector_id", subsector_id)
        .eq("leader_id", leader_id)
        .eq("roc_window", roc_window)
        .gte("date", start_str)
        .lte("date", end_str)
        .execute()
    )
    if not res.data:
        return None
    df = pd.DataFrame(res.data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df.rename(
        columns={
            "leader_norm": "Leader",
            "followers_avg_norm": "Followers_Avg",
            "negative_space": "Negative_Space",
            "roc_neg_space": "ROC_Negative_Space",
            "acc_neg_space": "ACC_Negative_Space",
            "phase": "Phase",
        }
    )
    return df

@st.cache_data(ttl="1d")
def get_stored_prices(ticker_ids: list[int], start_date: datetime, end_date: datetime):
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    all_prices = {}
    for tid in ticker_ids:
        res = (
            supabase.table("price_data")
            .select("*")
            .eq("ticker_id", tid)
            .gte("date", start_str)
            .lte("date", end_str)
            .execute()
        )
        if not res.data:
            continue
        tkr = supabase.table("tickers").select("symbol").eq("id", tid).execute()
        symbol = tkr.data[0]["symbol"]
        prices = pd.DataFrame(res.data)
        prices["date"] = pd.to_datetime(prices["date"])
        prices = prices.set_index("date")["adj_close"].astype(float)
        all_prices[symbol] = prices.sort_index()
    if not all_prices:
        return None
    df = pd.DataFrame(all_prices).sort_index()
    return df

# ==============================
# 4) Data Fetching from Yahoo
# ==============================
@st.cache_data
def fetch_stock_data(symbols: list[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    t = Ticker(symbols)
    hist = t.history(
        start=pd.to_datetime(start_date).strftime("%Y-%m-%d"),
        end=pd.to_datetime(end_date).strftime("%Y-%m-%d"),
        interval="1d",
    )
    if hist is None or (isinstance(hist, pd.DataFrame) and hist.empty):
        return pd.DataFrame()
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index().pivot(index="date", columns="symbol", values="adjclose")
    else:
        # single symbol comes back as a simple df
        hist = hist.rename(columns={"adjclose": symbols[0]})[[symbols[0]]]
    hist.index = pd.to_datetime(hist.index)
    hist = hist.sort_index().ffill().dropna(how="all")
    # Keep only requested symbols that actually returned
    cols_present = [c for c in symbols if c in hist.columns]
    return hist[cols_present] if cols_present else pd.DataFrame()

# ==============================
# 5) Sidebar Controls
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
    default_leader = LEADERS.get(sector, tickers[0]) if isinstance(LEADERS.get(sector), str) else tickers[0]

st.sidebar.caption(
    f"**Default Leader:** {default_leader}  \n"
    f"(*From the curated leader mapping for {subsector}*)"
)

# Allow override but default to mapped leader
leader = st.sidebar.selectbox("Leader (override optional)", tickers, index=tickers.index(default_leader))

# Date presets (defaults taken from your original script)
start_date = st.sidebar.date_input("Start Date", value=datetime(2023, 1, 1))
end_date = st.sidebar.date_input("End Date", value=datetime(2024, 7, 1))

roc_window = st.sidebar.slider("ROC Window (days)", min_value=5, max_value=30, value=14)
neg_space_threshold = st.sidebar.slider(
    "Negative Space Threshold",
    min_value=0.01,
    max_value=0.10,
    value=0.05,
    step=0.01,
)

# Data refresh option
refresh_data = st.sidebar.checkbox("Force refresh data", value=False)

# ==============================
# 6) IDs & Mappings
# ==============================
sector_id = get_sector_id(sector)
subsector_id = get_subsector_id(subsector, sector_id)
ticker_ids = [get_ticker_id(t) for t in tickers]
leader_id = get_ticker_id(leader)

ensure_subsector_tickers(subsector_id, ticker_ids, leader_id)

# ==============================
# 7) Try DB first, else Yahoo
# ==============================
stored_metrics = None if refresh_data else get_stored_metrics(
    subsector_id, leader_id, roc_window, start_date, end_date
)
stored_prices = None if refresh_data else get_stored_prices(
    ticker_ids, start_date, end_date
)

need_yahoo_data = refresh_data or stored_prices is None or stored_metrics is None

# ==============================
# 8) Sync Button
# ==============================
if st.sidebar.button("Sync Latest Data to Database"):
    sync_start = datetime.now() - timedelta(days=30)
    st.sidebar.info("Syncing latest data to database...")
    sync_data = fetch_stock_data(tickers, sync_start, datetime.now())
    if sync_data is None or sync_data.empty:
        st.sidebar.error("No fresh data returned from Yahoo. Skipping sync.")
    else:
        sync_ticker_ids = [get_ticker_id(t) for t in tickers]
        save_price_data(sync_ticker_ids, tickers, sync_data)

        sync_leader_prices = sync_data.get(leader)
        sync_followers = [s for s in sync_data.columns if s != leader]
        sync_follower_prices = sync_data[sync_followers] if sync_followers else pd.DataFrame(index=sync_data.index)

        if sync_leader_prices is None or sync_leader_prices.empty:
            st.sidebar.error("Leader data missing during sync; metrics not saved.")
        else:
            neg_space_s, leader_norm_s, avg_follower_norm_s = calc_neg_space(sync_leader_prices, sync_follower_prices)
            sync_roc_neg_space = roc(neg_space_s, roc_window)
            sync_acc_neg_space = roc(sync_roc_neg_space, roc_window)
            sync_phases = identify_phases(neg_space_s, sync_roc_neg_space, sync_acc_neg_space)

            sync_metrics_df = pd.DataFrame(
                {
                    "Leader": leader_norm_s,
                    "Followers_Avg": avg_follower_norm_s,
                    "Negative_Space": neg_space_s,
                    "ROC_Negative_Space": sync_roc_neg_space,
                    "ACC_Negative_Space": sync_acc_neg_space,
                    "Phase": sync_phases,
                }
            )
            save_sector_metrics(sector_id, subsector_id, leader_id, sync_metrics_df, roc_window)
            st.sidebar.success("Data sync complete!")

            # Invalidate caches
            get_stored_metrics.clear()
            get_stored_prices.clear()

# ==============================
# 9) Resolve Data for Current View
# =================# ==============================
# Imports & Setup
# ==============================
import os
import json
import warnings
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from supabase import create_client
from yahooquery import Ticker

warnings.filterwarnings("ignore")
load_dotenv()

# ==============================
# Streamlit Page Config
# ==============================
st.set_page_config(
    page_title="Sector Wave & Negative Space Dashboard",
    page_icon="📊",
    layout="wide",
)
st.title("📊 Sector Wave & Negative Space Dashboard")

# ==============================
# Supabase Configuration
# ==============================
@st.cache_resource
def init_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        st.error(
            "Supabase credentials not found. "
            "Please set SUPABASE_URL and SUPABASE_KEY in your .env file."
        )
        st.stop()
    return create_client(url, key)

supabase = init_supabase()

# ==============================
# 1) Sector, Ticker & Leader Mapping
# ==============================
SECTORS = {
    "Information Technology": {
        "AI": ["NVDA", "AMD", "PLTR", "SMCI", "SNOW", "AVGO"],
        "Cloud Computing": ["AMZN", "MSFT", "NET", "ORCL", "SMCI", "PLTR"],
        "Cybersecurity": ["CRWD", "ZS", "FTNT", "CYBR", "FFIV", "AVGO"],
    },
    "Healthcare": {
        "Telemedicine": ["TDOC", "DOCS", "AMWL", "PINC", "PHR", "IRTC"],
        "Biotechnology": ["MRNA", "REGN", "VRTX", "BTAI", "TNXP", "SNGX"],
        "Medical Devices": ["BSX", "MDT", "ABT", "SNN", "ZBH", "MASI"],
        "Health Data Analytics": ["TEM", "GEHC", "MDT", "BSX", "BDX", "GH"],
    },
    "Energy": {
        "Renewable Energy": ["NEE", "FSLR", "BEP", "ENLT", "RNW", "CEG"],
        "Energy Storage": ["STEM", "BE", "AEE", "CMS", "CEG", "ENPH"],
    },
    "Financials": {"Fintech": ["SQ", "SOFI", "PYPL", "TW", "HOOD", "IBKR"]},
    "Consumer Discretionary": {
        "E-commerce": ["AMZN", "SHOP", "WMT", "PYPL", "CVNA", "GRPN"],
        "Electric Vehicles": ["TSLA", "BYDDY", "NIO", "VWAGY", "RIVN", "LCID"],
    },
    "Industrials": {
        "Robotics & Automation": ["ISRG", "TXN", "ABB", "TER", "SYM", "PATH"],
        "Aerospace Technology": ["BA", "LMT", "RTX", "GD", "TDY", "HII"],
    },
    "Materials": {
        "Advanced Materials": ["EMN", "HTGC", "KMT", "RYAM", "HUN", "ASIX"],
        "Nanotechnology": ["ZTEK", "NNOMF", "NNXPF", "ATOM", "AVAV", "ONTO"],
        "Sustainable & Recycled Materials": ["EMN", "ECL", "SMG", "PKG", "WRK", "SEE"],
    },
    "Utilities": {
        "Smart Grid Technology": ["ITRI", "GRID", "IPWR", "BMI", "HASI", "ARQ"],
        "Renewable Electricity Integration": ["PEG", "NEE", "ED", "SO", "DUK", "ES"],
    },
    "Consumer Staples": {
        "Sustainable & Organic Food": ["NSRGY", "TSN", "CLX", "MO", "OLLI", "COCO"],
        "Household & Personal Care": ["PG", "CLX", "KMB", "UL", "CL", "EL"],
    },
    "Communication Services": {
        "Social Media & Interactive Media": ["META", "SNAP", "PINS", "SPOT", "BILI", "TWTR"],
        "Telecom Services": ["VZ", "T", "TMUS", "CMCSA", "DISH", "NOVN"],
    },
    "Real Estate": {
        "Proptech & Real Estate Tech": ["Z", "OPEN", "RDFN", "AURUMPROP", "COMP", "DOC"],
        "REITs (Retail & Industrial)": ["PLD", "AMT", "SPG", "PSA", "EQR", "DLR"],
    },
    "Basic Materials": {
        "Chemicals & Specialty Chemicals": ["DOW", "LYB", "SHW", "HUN", "ECL", "PPG"],
        "Metals & Mining": ["BHP", "RIO", "FCX", "NEM", "GOLD", "VALE"],
    },
    "Oil & Gas": {
        "Major Integrated Oil Companies": ["XOM", "CVX", "COP", "SLB", "WMB", "EOG"],
        "Oilfield Services & Equipment": ["SLB", "HAL", "BKR", "NOV", "FTI", "COS"],
        "Canadian Oil & Gas": ["MEG", "FO", "ATH", "HWX", "CNQ", "SU"],
    },
    "Gold & Precious Metals": {
        "Gold Mining": ["NEM", "GOLD", "KL", "AEM", "FNV", "ABX"],
        "Precious Metals Streaming & Royalty": ["FNV", "WPM", "RGLD", "HL", "PAAS", "AUY"],
    },
    "Arms/Defense": ["LMT", "RTX", "BA", "HON", "GD", "NOC"],
    "Cryptocurrency": ["COIN", "MSTR", "RIOT", "MARA", "BLOK", "CORE", "CLSK", "HUT", "HOOD", "TERA"],
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
    "Cryptocurrency": "COIN",
}

# ==============================
# 2) Helpers: Metrics & Phases
# ==============================
def normalize_prices(prices: pd.Series) -> pd.Series:
    if prices.isna().all():
        return pd.Series(index=prices.index, dtype=float)
    first = prices.dropna().iloc[0]
    if first == 0:
        return pd.Series(index=prices.index, dtype=float)
    return (prices / first - 1.0) * 100.0

def calc_neg_space(leader_prices: pd.Series, follower_prices: pd.DataFrame):
    leader_norm = normalize_prices(leader_prices)
    if follower_prices is None or follower_prices.empty:
        avg_follower_norm = pd.Series(0, index=leader_norm.index)
    else:
        follower_norms = pd.concat(
            [normalize_prices(follower_prices[c]) for c in follower_prices.columns],
            axis=1,
        )
        avg_follower_norm = follower_norms.mean(axis=1)
    neg_space = leader_norm - avg_follower_norm
    return neg_space, leader_norm, avg_follower_norm

def roc(series: pd.Series, win: int) -> pd.Series:
    if series is None or series.empty:
        return pd.Series(dtype=float)
    return series.pct_change(win) * 100.0

def identify_phases(neg_space: pd.Series, roc_ns: pd.Series, acc_ns: pd.Series):
    """
    Generic, non-proprietary phase rules:
    - Inactive: insufficient data or flat dynamics
    - Initiation: neg space > 0 and ROC > 0
    - Early Inflection: ROC < 0 and ACC < 0
    - Mid Inflection: ROC < 0 and ACC >= 0
    - Late Inflection: neg space <= 0 and ROC >= 0
    - Interruption: neg space > 0 and ROC > 0 after inflection
    """
    phases = []
    for i in range(len(neg_space)):
        ns = neg_space.iloc[i]
        r = roc_ns.iloc[i] if i < len(roc_ns) else np.nan
        a = acc_ns.iloc[i] if i < len(acc_ns) else np.nan
        if np.isnan(r) or np.isnan(a):
            phases.append("Inactive")
        elif r > 0 and ns > 0:
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

# ==============================
# 3) Database Operations
# ==============================
@st.cache_data(ttl="1h")
def get_sector_id(sector_name: str):
    res = supabase.table("sectors").select("id").eq("name", sector_name).execute()
    if res.data:
        return res.data[0]["id"]
    ins = supabase.table("sectors").insert({"name": sector_name}).execute()
    return ins.data[0]["id"]

@st.cache_data(ttl="1h")
def get_subsector_id(subsector_name: str, sector_id: int):
    res = (
        supabase.table("subsectors")
        .select("id")
        .eq("name", subsector_name)
        .eq("sector_id", sector_id)
        .execute()
    )
    if res.data:
        return res.data[0]["id"]
    ins = supabase.table("subsectors").insert({"name": subsector_name, "sector_id": sector_id}).execute()
    return ins.data[0]["id"]

@st.cache_data(ttl="1h")
def get_ticker_id(ticker_symbol: str):
    res = supabase.table("tickers").select("id").eq("symbol", ticker_symbol).execute()
    if res.data:
        return res.data[0]["id"]
    ins = supabase.table("tickers").insert({"symbol": ticker_symbol}).execute()
    return ins.data[0]["id"]

def ensure_subsector_tickers(subsector_id: int, ticker_ids: list[int], leader_id: int):
    for tid in ticker_ids:
        res = (
            supabase.table("subsector_tickers")
            .select("id,is_leader")
            .eq("subsector_id", subsector_id)
            .eq("ticker_id", tid)
            .execute()
        )
        if not res.data:
            supabase.table("subsector_tickers").insert(
                {"subsector_id": subsector_id, "ticker_id": tid, "is_leader": tid == leader_id}
            ).execute()
        else:
            mapping_id = res.data[0]["id"]
            supabase.table("subsector_tickers").update(
                {"is_leader": tid == leader_id}
            ).eq("id", mapping_id).execute()

def save_price_data(ticker_ids: list[int], symbols: list[str], data: pd.DataFrame):
    for symbol, ticker_id in zip(symbols, ticker_ids):
        if symbol not in data.columns:
            continue
        prices = data[symbol].dropna()
        if prices.empty:
            continue
        records = []
        for date, price in prices.items():
            ds = pd.to_datetime(date).strftime("%Y-%m-%d")
            exists = (
                supabase.table("price_data")
                .select("id")
                .eq("ticker_id", ticker_id)
                .eq("date", ds)
                .execute()
            )
            if not exists.data:
                records.append({"ticker_id": ticker_id, "date": ds, "adj_close": float(price)})
        for i in range(0, len(records), 100):
            batch = records[i : i + 100]
            if batch:
                supabase.table("price_data").insert(batch).execute()

def save_sector_metrics(sector_id: int, subsector_id: int, leader_id: int, metrics_df: pd.DataFrame, roc_window: int):
    records = []
    for idx, row in metrics_df.iterrows():
        ds = pd.to_datetime(idx).strftime("%Y-%m-%d")
        exists = (
            supabase.table("sector_metrics")
            .select("id")
            .eq("subsector_id", subsector_id)
            .eq("leader_id", leader_id)
            .eq("date", ds)
            .eq("roc_window", roc_window)
            .execute()
        )
        if not exists.data:
            records.append(
                {
                    "sector_id": sector_id,
                    "subsector_id": subsector_id,
                    "leader_id": leader_id,
                    "date": ds,
                    "roc_window": roc_window,
                    "leader_norm": float(row.get("Leader", np.nan)) if pd.notna(row.get("Leader", np.nan)) else None,
                    "followers_avg_norm": float(row.get("Followers_Avg", np.nan)) if pd.notna(row.get("Followers_Avg", np.nan)) else None,
                    "negative_space": float(row.get("Negative_Space", np.nan)) if pd.notna(row.get("Negative_Space", np.nan)) else None,
                    "roc_neg_space": float(row.get("ROC_Negative_Space", np.nan)) if pd.notna(row.get("ROC_Negative_Space", np.nan)) else None,
                    "acc_neg_space": float(row.get("ACC_Negative_Space", np.nan)) if pd.notna(row.get("ACC_Negative_Space", np.nan)) else None,
                    "phase": row.get("Phase", None),
                }
            )
    for i in range(0, len(records), 100):
        batch = records[i : i + 100]
        if batch:
            supabase.table("sector_metrics").insert(batch).execute()

@st.cache_data(ttl="1d")
def get_stored_metrics(subsector_id: int, leader_id: int, roc_window: int, start_date: datetime, end_date: datetime):
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    res = (
        supabase.table("sector_metrics")
        .select("*")
        .eq("subsector_id", subsector_id)
        .eq("leader_id", leader_id)
        .eq("roc_window", roc_window)
        .gte("date", start_str)
        .lte("date", end_str)
        .execute()
    )
    if not res.data:
        return None
    df = pd.DataFrame(res.data)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    df = df.rename(
        columns={
            "leader_norm": "Leader",
            "followers_avg_norm": "Followers_Avg",
            "negative_space": "Negative_Space",
            "roc_neg_space": "ROC_Negative_Space",
            "acc_neg_space": "ACC_Negative_Space",
            "phase": "Phase",
        }
    )
    return df

@st.cache_data(ttl="1d")
def get_stored_prices(ticker_ids: list[int], start_date: datetime, end_date: datetime):
    start_str = pd.to_datetime(start_date).strftime("%Y-%m-%d")
    end_str = pd.to_datetime(end_date).strftime("%Y-%m-%d")
    all_prices = {}
    for tid in ticker_ids:
        res = (
            supabase.table("price_data")
            .select("*")
            .eq("ticker_id", tid)
            .gte("date", start_str)
            .lte("date", end_str)
            .execute()
        )
        if not res.data:
            continue
        tkr = supabase.table("tickers").select("symbol").eq("id", tid).execute()
        symbol = tkr.data[0]["symbol"]
        prices = pd.DataFrame(res.data)
        prices["date"] = pd.to_datetime(prices["date"])
        prices = prices.set_index("date")["adj_close"].astype(float)
        all_prices[symbol] = prices.sort_index()
    if not all_prices:
        return None
    df = pd.DataFrame(all_prices).sort_index()
    return df

# ==============================
# 4) Data Fetching from Yahoo
# ==============================
@st.cache_data
def fetch_stock_data(symbols: list[str], start_date: datetime, end_date: datetime) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    t = Ticker(symbols)
    hist = t.history(
        start=pd.to_datetime(start_date).strftime("%Y-%m-%d"),
        end=pd.to_datetime(end_date).strftime("%Y-%m-%d"),
        interval="1d",
    )
    if hist is None or (isinstance(hist, pd.DataFrame) and hist.empty):
        return pd.DataFrame()
    if isinstance(hist.index, pd.MultiIndex):
        hist = hist.reset_index().pivot(index="date", columns="symbol", values="adjclose")
    else:
        # single symbol comes back as a simple df
        hist = hist.rename(columns={"adjclose": symbols[0]})[[symbols[0]]]
    hist.index = pd.to_datetime(hist.index)
    hist = hist.sort_index().ffill().dropna(how="all")
    # Keep only requested symbols that actually returned
    cols_present = [c for c in symbols if c in hist.columns]
    return hist[cols_present] if cols_present else pd.DataFrame()

# ==============================
# 5) Sidebar Controls
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
    default_leader = LEADERS.get(sector, tickers[0]) if isinstance(LEADERS.get(sector), str) else tickers[0]

st.sidebar.caption(
    f"**Default Leader:** {default_leader}  \n"
    f"(*From the curated leader mapping for {subsector}*)"
)

# Allow override but default to mapped leader
leader = st.sidebar.selectbox("Leader (override optional)", tickers, index=tickers.index(default_leader))

# Date presets (defaults taken from your original script)
start_date = st.sidebar.date_input("Start Date", value=datetime(2023, 1, 1))
end_date = st.sidebar.date_input("End Date", value=datetime(2024, 7, 1))

roc_window = st.sidebar.slider("ROC Window (days)", min_value=5, max_value=30, value=14)
neg_space_threshold = st.sidebar.slider(
    "Negative Space Threshold",
    min_value=0.01,
    max_value=0.10,
    value=0.05,
    step=0.01,
)

# Data refresh option
refresh_data = st.sidebar.checkbox("Force refresh data", value=False)

# ==============================
# 6) IDs & Mappings
# ==============================
sector_id = get_sector_id(sector)
subsector_id = get_subsector_id(subsector, sector_id)
ticker_ids = [get_ticker_id(t) for t in tickers]
leader_id = get_ticker_id(leader)

ensure_subsector_tickers(subsector_id, ticker_ids, leader_id)

# ==============================
# 7) Try DB first, else Yahoo
# ==============================
stored_metrics = None if refresh_data else get_stored_metrics(
    subsector_id, leader_id, roc_window, start_date, end_date
)
stored_prices = None if refresh_data else get_stored_prices(
    ticker_ids, start_date, end_date
)

need_yahoo_data = refresh_data or stored_prices is None or stored_metrics is None

# ==============================
# 8) Sync Button
# ==============================
if st.sidebar.button("Sync Latest Data to Database"):
    sync_start = datetime.now() - timedelta(days=30)
    st.sidebar.info("Syncing latest data to database...")
    sync_data = fetch_stock_data(tickers, sync_start, datetime.now())
    if sync_data is None or sync_data.empty:
        st.sidebar.error("No fresh data returned from Yahoo. Skipping sync.")
    else:
        sync_ticker_ids = [get_ticker_id(t) for t in tickers]
        save_price_data(sync_ticker_ids, tickers, sync_data)

        sync_leader_prices = sync_data.get(leader)
        sync_followers = [s for s in sync_data.columns if s != leader]
        sync_follower_prices = sync_data[sync_followers] if sync_followers else pd.DataFrame(index=sync_data.index)

        if sync_leader_prices is None or sync_leader_prices.empty:
            st.sidebar.error("Leader data missing during sync; metrics not saved.")
        else:
            neg_space_s, leader_norm_s, avg_follower_norm_s = calc_neg_space(sync_leader_prices, sync_follower_prices)
            sync_roc_neg_space = roc(neg_space_s, roc_window)
            sync_acc_neg_space = roc(sync_roc_neg_space, roc_window)
            sync_phases = identify_phases(neg_space_s, sync_roc_neg_space, sync_acc_neg_space)

            sync_metrics_df = pd.DataFrame(
                {
                    "Leader": leader_norm_s,
                    "Followers_Avg": avg_follower_norm_s,
                    "Negative_Space": neg_space_s,
                    "ROC_Negative_Space": sync_roc_neg_space,
                    "ACC_Negative_Space": sync_acc_neg_space,
                    "Phase": sync_phases,
                }
            )
            save_sector_metrics(sector_id, subsector_id, leader_id, sync_metrics_df, roc_window)
            st.sidebar.success("Data sync complete!")

            # Invalidate caches
            get_stored_metrics.clear()
            get_stored_prices.clear()

# ==============================
# 9) Resolve Data for Current View
# =================
if need_yahoo_data:
    data = fetch_stock_data(tickers, start_date, end_date)
    if data is None or data.empty:
        st.error("Failed to fetch data. Please check ticker symbols and date range.")
        st.stop()
    save_price_data(ticker_ids, tickers, data)
else:
    data = stored_prices
    if data is None or data.empty:
        st.error("No data available in database. Please try refreshing the data.")
        st.stop()

# Build metrics (from DB if present, else compute & save)
if stored_metrics is not None and not refresh_data:
    metrics_df = stored_metrics.copy()
    leader_norm = metrics_df["Leader"]
    avg_follower_norm = metrics_df["Followers_Avg"]
    neg_space = metrics_df["Negative_Space"]
    roc_neg_space = metrics_df["ROC_Negative_Space"]
    acc_neg_space = metrics_df["ACC_Negative_Space"]
    phases = metrics_df["Phase"].tolist()
else:
    leader_prices = data[leader] if leader in data.columns else pd.Series(dtype=float, index=data.index)
    followers = [s for s in data.columns if s != leader]
    follower_prices = data[followers] if followers else pd.DataFrame(index=data.index)

    neg_space, leader_norm, avg_follower_norm = calc_neg_space(leader_prices, follower_prices)
    roc_neg_space = roc(neg_space, roc_window)
    acc_neg_space = roc(roc_neg_space, roc_window)
    phases = identify_phases(neg_space, roc_neg_space, acc_neg_space)

    metrics_df = pd.DataFrame(
        {
            "Leader": leader_norm,
            "Followers_Avg": avg_follower_norm,
            "Negative_Space": neg_space,
            "ROC_Negative_Space": roc_neg_space,
            "ACC_Negative_Space": acc_neg_space,
            "Phase": phases,
        }
    )
    save_sector_metrics(sector_id, subsector_id, leader_id, metrics_df, roc_window)
    
# ==============================
# 10) Main Display (Charts)
# ==============================
st.subheader("Normalized Prices (Leader vs Followers Avg)")
norm_df = pd.DataFrame(
    {
        f"{leader} (Leader)": leader_norm,
        "Followers Avg": avg_follower_norm,
    }
).dropna(how="all")
if not norm_df.empty:
    st.line_chart(norm_df)
else:
    st.info("No normalized price data to display.")

st.subheader("Negative Space & ROC")
fig_neg = go.Figure()
fig_neg.add_trace(
    go.Scatter(
        x=metrics_df.index,
        y=neg_space,
        name="Negative Space",
        mode="lines",
    )
)
fig_neg.add_trace(
    go.Scatter(
        x=metrics_df.index,
        y=roc_neg_space,
        name="ROC (Negative Space)",
        mode="lines",
    )
)
fig_neg.add_hline(y=0, line_dash="dot", line_color="gray")
fig_neg.update_layout(
    height=420,
    legend_orientation="h",
    margin=dict(l=10, r=10, t=30, b=10),
)
st.plotly_chart(fig_neg, use_container_width=True)

# Phase badge
st.subheader("Current Phase")
if len(phases) > 0:
    st.markdown(f"**{phases[-1]}**")
else:
    st.markdown("**Inactive**")

# ==============================
# 11) Vertical Dot Plot: Progress Toward Inflection
# ==============================
st.subheader("Vertical Dot Plot: Progress Toward Inflection (0 → 1)")

leader_max = max(float(leader_norm.max()) if not leader_norm.empty else 0.0, 1e-9)

def series_progress(s: pd.Series) -> pd.Series:
    n = normalize_prices(s)
    return (n / leader_max).clip(lower=0, upper=1)

progress_df = pd.DataFrame({c: series_progress(data[c]) for c in data.columns if c in data}).dropna(how="all")
latest_progress = progress_df.iloc[-1] if not progress_df.empty else pd.Series(dtype=float)

fig_dot = go.Figure()
guides = [
    (0.15, "Initiation zone"),
    (0.40, "Early Inflection zone"),
    (0.65, "Mid Inflection zone"),
    (0.85, "Late Inflection zone"),
]
for y, label in guides:
    fig_dot.add_hline(y=y, line_dash="dot", opacity=0.4)
    fig_dot.add_annotation(
        xref="paper",
        x=1.0,
        y=y,
        text=label,
        showarrow=False,
        xanchor="right",
        yanchor="bottom",
        font=dict(size=10),
    )

for stock in data.columns:
    val = float(latest_progress.get(stock, np.nan)) if not np.isnan(latest_progress.get(stock, np.nan)) else None
    if val is None:
        continue
    is_leader = stock == leader
    fig_dot.add_trace(
        go.Scatter(
            x=[stock],
            y=[val],
            mode="markers",
            marker=dict(size=14, symbol="circle", line=dict(width=1)),
            name=f"{stock}{' (Leader)' if is_leader else ''}",
            hovertemplate=f"<b>{stock}</b><br>Progress: {val:.2f}<br>Phase: {phases[-1] if phases else 'Inactive'}<extra></extra>",
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
# 12) Constituents & Leader Note
# ==============================
with st.expander("Constituents & Leader Mapping"):
    if isinstance(SECTORS[sector], dict):
        st.write(f"**{sector} → {subsector}** constituents:")
        st.write(", ".join(tickers))
        st.write(f"**Mapped Leader:** {LEADERS.get(sector, {}).get(subsector, default_leader)}")
    else:
        st.write(f"**{sector}** constituents:")
        st.write(", ".join(tickers))
        mapped_leader = LEADERS.get(sector, default_leader) if isinstance(LEADERS.get(sector), str) else default_leader
        st.write(f"**Mapped Leader:** {mapped_leader}")

# ==============================
# 13) Database Status
# ==============================
with st.expander("Database Status"):
    st.write("**Database Connection:**", "✅ Connected" if supabase else "❌ Not Connected")
    try:
        price_count_res = supabase.table("price_data").select("id", count="exact").execute()
        metrics_count_res = supabase.table("sector_metrics").select("id", count="exact").execute()
        price_count = getattr(price_count_res, "count", None)
        metrics_count = getattr(metrics_count_res, "count", None)
        st.write(f"**Price Records:** {price_count if price_count is not None else 'Unknown'}")
        st.write(f"**Metrics Records:** {metrics_count if metrics_count is not None else 'Unknown'}")
    except Exception as e:
        st.write("Count query failed:", str(e))

    if data is not None and not data.empty:
        st.write(f"**Latest Data Date:** {pd.to_datetime(data.index.max()).strftime('%Y-%m-%d')}")

# ==============================
# 14) Download Metrics
# ==============================
st.download_button(
    "Download Metrics CSV",
    metrics_df.to_csv(index=True),
    file_name=f"{sector}_{subsector}_metrics.csv",
    mime="text/csv",
)

# ==============================
# 15) Batch Processing Option
# ==============================
with st.expander("Batch Process All Sectors"):
    st.write("This processes the **last 30 days** for every (sub)sector and stores prices + metrics.")
    if st.button("Process All Sectors (Warning: Time-Intensive)"):
        progress_bar = st.progress(0)
        status_text = st.empty()

        # Count total groups for progress (all subsectors + flat lists)
        total_groups = 0
        for s, v in SECTORS.items():
            if isinstance(v, dict):
                total_groups += len(v)
            else:
                total_groups += 1

        processed = 0
        for batch_sector, value in SECTORS.items():
            if isinstance(value, dict):  # Has subsectors
                for batch_subsector, batch_tickers in value.items():
                    status_text.text(f"Processing {batch_sector} → {batch_subsector}...")
                    batch_leader = LEADERS.get(batch_sector, {}).get(batch_subsector, batch_tickers[0])

                    batch_start = datetime.now() - timedelta(days=30)
                    batch_data = fetch_stock_data(batch_tickers, batch_start, datetime.now())
                    if batch_data is None or batch_data.empty:
                        processed += 1
                        progress_bar.progress(processed / total_groups)
                        continue

                    batch_sector_id = get_sector_id(batch_sector)
                    batch_subsector_id = get_subsector_id(batch_subsector, batch_sector_id)
                    batch_ticker_ids = [get_ticker_id(t) for t in batch_tickers]
                    batch_leader_id = get_ticker_id(batch_leader)
                    ensure_subsector_tickers(batch_subsector_id, batch_ticker_ids, batch_leader_id)

                    save_price_data(batch_ticker_ids, batch_tickers, batch_data)

                    blp = batch_data[batch_leader] if batch_leader in batch_data.columns else pd.Series(dtype=float, index=batch_data.index)
                    followers = [s for s in batch_data.columns if s != batch_leader]
                    bfp = batch_data[followers] if followers else pd.DataFrame(index=batch_data.index)

                    ns, ln, fan = calc_neg_space(blp, bfp)
                    r1 = roc(ns, roc_window)
                    r2 = roc(r1, roc_window)
                    ph = identify_phases(ns, r1, r2)

                    batch_metrics_df = pd.DataFrame(
                        {
                            "Leader": ln,
                            "Followers_Avg": fan,
                            "Negative_Space": ns,
                            "ROC_Negative_Space": r1,
                            "ACC_Negative_Space": r2,
                            "Phase": ph,
                        }
                    )
                    save_sector_metrics(batch_sector_id, batch_subsector_id, batch_leader_id, batch_metrics_df, roc_window)

                    processed += 1
                    progress_bar.progress(processed / total_groups)
            else:  # Flat sector list
                batch_subsector = batch_sector
                batch_tickers = value
                status_text.text(f"Processing {batch_sector}...")
                batch_leader = LEADERS.get(batch_sector, batch_tickers[0]) if isinstance(LEADERS.get(batch_sector), str) else batch_tickers[0]

                batch_start = datetime.now() - timedelta(days=30)
                batch_data = fetch_stock_data(batch_tickers, batch_start, datetime.now())
                if batch_data is None or batch_data.empty:
                    processed += 1
                    progress_bar.progress(processed / total_groups)
                    continue

                batch_sector_id = get_sector_id(batch_sector)
                batch_subsector_id = get_subsector_id(batch_subsector, batch_sector_id)
                batch_ticker_ids = [get_ticker_id(t) for t in batch_tickers]
                batch_leader_id = get_ticker_id(batch_leader)
                ensure_subsector_tickers(batch_subsector_id, batch_ticker_ids, batch_leader_id)

                save_price_data(batch_ticker_ids, batch_tickers, batch_data)

                blp = batch_data[batch_leader] if batch_leader in batch_data.columns else pd.Series(dtype=float, index=batch_data.index)
                followers = [s for s in batch_data.columns if s != batch_leader]
                bfp = batch_data[followers] if followers else pd.DataFrame(index=batch_data.index)

                ns, ln, fan = calc_neg_space(blp, bfp)
                r1 = roc(ns, roc_window)
                r2 = roc(r1, roc_window)
                ph = identify_phases(ns, r1, r2)

                batch_metrics_df = pd.DataFrame(
                    {
                        "Leader": ln,
                        "Followers_Avg": fan,
                        "Negative_Space": ns,
                        "ROC_Negative_Space": r1,
                        "ACC_Negative_Space": r2,
                        "Phase": ph,
                    }
                )
                save_sector_metrics(batch_sector_id, batch_subsector_id, batch_leader_id, batch_metrics_df, roc_window)

                processed += 1
                progress_bar.progress(processed / total_groups)

        # Clear caches after batch processing
        get_stored_metrics.clear()
        get_stored_prices.clear()
        status_text.text("Batch processing complete!")
        st.success("All sectors processed and saved to database.")
