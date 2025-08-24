# ==============================
# Imports & Setup
# ==============================
import streamlit as st
import pandas as pd
import numpy as np
from yahooquery import Ticker
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from datetime import datetime, timedelta
import warnings

warnings.filterwarnings('ignore')

# ==============================
# Streamlit Page Config
# ==============================
st.set_page_config(
    page_title="Sector Wave Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

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
    }
}
# ==============================
# Remaining Sectors & Leaders
# ==============================
SECTORS.update({
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
})

LEADERS.update({
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
})

# ==============================
# Database Setup
# ==============================
import sqlite3
conn = sqlite3.connect("sector_wave.db", check_same_thread=False)
cursor = conn.cursor()

cursor.executescript("""
CREATE TABLE IF NOT EXISTS sectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
);
CREATE TABLE IF NOT EXISTS subsectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    sector_id INTEGER,
    UNIQUE(name, sector_id),
    FOREIGN KEY(sector_id) REFERENCES sectors(id)
);
CREATE TABLE IF NOT EXISTS tickers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE,
    name TEXT,
    sector_id INTEGER,
    FOREIGN KEY(sector_id) REFERENCES sectors(id)
);
CREATE TABLE IF NOT EXISTS subsector_tickers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    subsector_id INTEGER,
    ticker_id INTEGER,
    is_leader BOOLEAN DEFAULT 0,
    UNIQUE(subsector_id, ticker_id),
    FOREIGN KEY(subsector_id) REFERENCES subsectors(id),
    FOREIGN KEY(ticker_id) REFERENCES tickers(id)
);
CREATE TABLE IF NOT EXISTS price_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker_id INTEGER,
    date TEXT,
    adj_close REAL,
    UNIQUE(ticker_id, date),
    FOREIGN KEY(ticker_id) REFERENCES tickers(id)
);
CREATE TABLE IF NOT EXISTS sector_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sector_id INTEGER,
    subsector_id INTEGER,
    leader_id INTEGER,
    date TEXT,
    roc_window INTEGER,
    leader_norm REAL,
    followers_avg_norm REAL,
    negative_space REAL,
    roc_neg_space REAL,
    acc_neg_space REAL,
    phase TEXT,
    UNIQUE(subsector_id, leader_id, date, roc_window),
    FOREIGN KEY(sector_id) REFERENCES sectors(id),
    FOREIGN KEY(subsector_id) REFERENCES subsectors(id),
    FOREIGN KEY(leader_id) REFERENCES tickers(id)
);
""")
conn.commit()

# ==============================
# Price Fetching & Update
# ==============================
import yfinance as yf
from datetime import datetime, timedelta

def update_prices(tickers):
    for symbol in tickers:
        data = yf.download(symbol, period="1y", interval="1d")
        for date, row in data.iterrows():
            cursor.execute("""
                INSERT OR IGNORE INTO price_data (ticker_id, date, adj_close)
                SELECT t.id, ?, ? FROM tickers t WHERE t.symbol = ?
            """, (date.strftime("%Y-%m-%d"), row['Adj Close'], symbol))
    conn.commit()

all_tickers = set()
for sector, subsectors in SECTORS.items():
    for tick_list in subsectors.values():
        all_tickers.update(tick_list)
update_prices(list(all_tickers))

# ==============================
# Metrics Calculation
# ==============================
import pandas as pd
import numpy as np

def calculate_metrics(subsector_name, roc_window=20):
    cursor.execute("""
        SELECT t.id, t.symbol FROM tickers t
        JOIN subsector_tickers st ON t.id = st.ticker_id
        JOIN subsectors s ON s.id = st.subsector_id
        WHERE s.name = ?
    """, (subsector_name,))
    tickers = cursor.fetchall()
    
    df_prices = pd.DataFrame()
    for tid, symbol in tickers:
        df = pd.read_sql("""
            SELECT date, adj_close FROM price_data
            WHERE ticker_id = ?
            ORDER BY date
        """, conn, params=(tid,))
        df = df.rename(columns={'adj_close': symbol}).set_index('date')
        df_prices = pd.concat([df_prices, df], axis=1)

    if df_prices.empty:
        return None

    roc = df_prices.pct_change(periods=roc_window)
    metrics_list = []
    for symbol in df_prices.columns:
        leader = LEADERS.get(subsector_name.split()[0], {}).get(subsector_name, symbol)
        leader_series = roc.get(leader, pd.Series(0))
        followers = [col for col in df_prices.columns if col != leader]
        followers_avg = roc[followers].mean(axis=1)
        neg_space = leader_series - followers_avg
        metrics_list.append((subsector_name, leader, leader_series.iloc[-1],
                             followers_avg.iloc[-1], neg_space.iloc[-1]))
    return metrics_list

# ==============================
# Streamlit Dashboard
# ==============================
import streamlit as st
st.set_page_config(page_title="Sector Wave Dashboard", layout="wide")

st.title("Sector Wave Detection Dashboard")
subsector = st.selectbox("Select Subsector", [s for sector in SECTORS.values() for s in sector.keys()])
roc_window = st.slider("ROC Window (Days)", 5, 60, 20)
metrics = calculate_metrics(subsector, roc_window)

if metrics:
    for m in metrics:
        st.write(f"Subsector: {m[0]}")
        st.write(f"Leader: {m[1]}")
        st.write(f"Leader Norm: {m[2]:.4f}")
        st.write(f"Followers Avg Norm: {m[3]:.4f}")
        st.write(f"Negative Space: {m[4]:.4f}")
else:
    st.write("No price data available for this subsector.")
