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
from supabase import create_client
from yahooquery import Ticker

warnings.filterwarnings("ignore")

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
    try:
        url = "https://dzddytphimhoxeccxqsw.supabase.co"
        key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
        
        client = create_client(url, key)
        # Test connection
        client.table("sectors").select("count", count="exact").execute()
        return client
    except Exception as e:
        st.error(f"Failed to connect to Supabase: {str(e)}")
        st.stop()
        return None

supabase = init_supabase()

if supabase:
    st.sidebar.success("✅ Database Connected")
else:
    st.sidebar.error("❌ Database Not Connected")

# ==============================
# Sector, Ticker & Leader Mapping
# ==============================
# ==============================
# Sector, Ticker & Leader Mapping
# ==============================
SECTORS = {
    "Technology & AI": {
        "Artificial Intelligence": ["NVDA", "AMD", "PLTR", "MSFT", "GOOG", "AI"],
        "Semiconductors": ["NVDA", "AMD", "INTC", "TSM", "AMAT", "MU"],
        "Cloud Computing": ["MSFT", "AMZN", "GOOGL", "CRM", "NET", "SNOW"],
        "Cybersecurity": ["CRWD", "PANW", "ZS", "FTNT", "S", "GEN"]
    },
    "Clean Energy & EV": {
        "Electric Vehicles": ["TSLA", "RIVN", "LCID", "NIO", "LI", "XPEV"],
        "Battery Technology": ["ALB", "LTHM", "LAC", "SQM", "PLL", "FREY"],
        "Solar": ["SEDG", "ENPH", "FSLR", "SPWR", "NOVA", "MAXN"],
        "Wind & Renewables": ["NEE", "DNNGY", "DQ", "VWDRY", "CWEN", "BEP"]
    },
    "Biotech & Healthcare": {
        "Biotech Innovation": ["MRNA", "BNTX", "CRSP", "EDIT", "NTLA", "BEAM"],
        "Medical Devices": ["ISRG", "MDT", "BSX", "EW", "ZBH", "ABMD"],
        "Digital Health": ["TDOC", "DOCS", "ONEM", "AMWL", "ACCD", "PHR"],
        "Genomics": ["DNA", "PACB", "TWST", "BLI", "TXG", "ME"]
    },
    "Financial Innovation": {
        "Fintech": ["SQ", "PYPL", "COIN", "HOOD", "SOFI", "UPST"],
        "Digital Payments": ["V", "MA", "PYPL", "SQ", "ADYEY", "AFRM"],
        "Blockchain": ["COIN", "MSTR", "MARA", "RIOT", "HUT", "BITF"],
        "Neo-Banking": ["SOFI", "NU", "DAVE", "PSEC", "LC", "OCFT"]
    },
    "Web3 & Metaverse": {
        "Gaming": ["RBLX", "U", "TTWO", "EA", "ATVI", "PLTK"],
        "Social Platforms": ["META", "SNAP", "PINS", "MTCH", "BMBL", "HOOD"],
        "Digital Assets": ["COIN", "SI", "MSTR", "BITF", "HUT", "RIOT"],
        "AR/VR": ["META", "SNAP", "U", "MTTR", "IMMR", "VUZI"]
    },
    "Space & Defense": {
        "Space Technology": ["SPCE", "RKLB", "ASTR", "MNTS", "SATL", "RDW"],
        "Defense": ["LMT", "RTX", "NOC", "GD", "BA", "HII"],
        "Satellite Communications": ["IRDM", "GSAT", "MAXR", "VSAT", "SATS", "LLAP"],
        "Aerospace": ["BA", "AIR", "TDG", "HEI", "SPR", "AJRD"]
    },
    "Future Materials": {
        "Advanced Materials": ["DD", "CE", "PPG", "ALB", "CTVA", "ECL"],
        "Rare Earth Elements": ["MP", "LTHM", "LAC", "UUUU", "CCJ", "DNN"],
        "Nanotechnology": ["NANO", "CAMT", "FORM", "NCTY", "WATT", "NNDM"],
        "Green Materials": ["OC", "TREX", "AZEK", "JCI", "TTEK", "CLH"]
    },
    "Infrastructure": {
        "Smart Cities": ["BLDR", "PWR", "DY", "MTZ", "WIRE", "AGX"],
        "5G Networks": ["COMM", "BAND", "ERIC", "NOK", "AVNW", "IDCC"],
        "Grid Modernization": ["AEE", "NEE", "DUK", "SO", "PCG", "EIX"],
        "Construction Tech": ["PRIM", "ACM", "FLR", "PWR", "DY", "MTZ"]
    }
}

LEADERS = {
    "Technology & AI": {
        "Artificial Intelligence": "NVDA",
        "Semiconductors": "NVDA",
        "Cloud Computing": "MSFT",
        "Cybersecurity": "CRWD"
    },
    "Clean Energy & EV": {
        "Electric Vehicles": "TSLA",
        "Battery Technology": "ALB",
        "Solar": "SEDG",
        "Wind & Renewables": "NEE"
    },
    "Biotech & Healthcare": {
        "Biotech Innovation": "MRNA",
        "Medical Devices": "ISRG",
        "Digital Health": "TDOC",
        "Genomics": "DNA"
    },
    "Financial Innovation": {
        "Fintech": "SQ",
        "Digital Payments": "V",
        "Blockchain": "COIN",
        "Neo-Banking": "SOFI"
    },
    "Web3 & Metaverse": {
        "Gaming": "RBLX",
        "Social Platforms": "META",
        "Digital Assets": "COIN",
        "AR/VR": "META"
    },
    "Space & Defense": {
        "Space Technology": "SPCE",
        "Defense": "LMT",
        "Satellite Communications": "IRDM",
        "Aerospace": "BA"
    },
    "Future Materials": {
        "Advanced Materials": "DD",
        "Rare Earth Elements": "MP",
        "Nanotechnology": "NANO",
        "Green Materials": "OC"
    },
    "Infrastructure": {
        "Smart Cities": "BLDR",
        "5G Networks": "COMM",
        "Grid Modernization": "AEE",
        "Construction Tech": "PRIM"
    }
}

# ==============================
# Helpers: Metrics & Phases
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
# Database Operations
# ==============================
@st.cache_data(ttl="1h")
def get_sector_id(sector_name: str):
    try:
        res = supabase.table("sectors").select("id").eq("name", sector_name).execute()
        if res.data:
            return res.data[0]["id"]
        ins = supabase.table("sectors").insert({"name": sector_name}).execute()
        return ins.data[0]["id"]
    except Exception as e:
        st.error(f"Error getting sector ID: {str(e)}")
        return None

@st.cache_data(ttl="1h")
def get_subsector_id(subsector_name: str, sector_id: int):
    try:
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
    except Exception as e:
        st.error(f"Error getting subsector ID: {str(e)}")
        return None

@st.cache_data(ttl="1h")
def get_ticker_id(ticker_symbol: str):
    try:
        res = supabase.table("tickers").select("id").eq("symbol", ticker_symbol).execute()
        if res.data:
            return res.data[0]["id"]
        ins = supabase.table("tickers").insert({"symbol": ticker_symbol}).execute()
        return ins.data[0]["id"]
    except Exception as e:
        st.error(f"Error getting ticker ID: {str(e)}")
        return None

def ensure_subsector_tickers(subsector_id: int, ticker_ids: list[int], leader_id: int):
    try:
        for tid in ticker_ids:
            try:
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
            except Exception as e:
                st.warning(f"Error processing ticker {tid}: {str(e)}")
                continue
    except Exception as e:
        st.error(f"Error ensuring subsector tickers: {str(e)}")

# ==============================
# Sidebar Controls & Data Processing
# ==============================
sector = st.sidebar.selectbox("Select Sector", list(SECTORS.keys()))

if isinstance(SECTORS[sector], dict):
    subsector_list = list(SECTORS[sector].keys())
    subsector = st.sidebar.selectbox("Select Niche/Subsector", subsector_list)
        
