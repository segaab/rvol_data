# ==============================
# Sector Wave Dashboard - Part 1: Database Setup & Tickers
# ==============================

import sqlite3
from yahooquery import Ticker

# ------------------------------
# Database Setup
# ------------------------------
conn = sqlite3.connect("sector_wave.db", check_same_thread=False)
cursor = conn.cursor()

cursor.executescript("""
CREATE TABLE IF NOT EXISTS sectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE,
    symbol TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS tickers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT UNIQUE,
    name TEXT,
    sector_id INTEGER,
    FOREIGN KEY(sector_id) REFERENCES sectors(id)
);

CREATE TABLE IF NOT EXISTS subsectors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    sector_id INTEGER,
    UNIQUE(name, sector_id),
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

CREATE TABLE IF NOT EXISTS price_data_an (
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

# ------------------------------
# Insert sectors, subsectors, and tickers
# ------------------------------
# (SECTORS, LEADERS, and tickers with company names as you provided)
# Paste your SECTORS dict, LEADERS dict, and the INSERT INTO tickers (...) block here

conn.commit()
conn.close()
print("✅ Database setup and tickers inserted successfully")

# ==============================
# Sector Wave Dashboard - Part 2: Data Fetching & Metrics
# ==============================

import sqlite3
import pandas as pd
import numpy as np
from yahooquery import Ticker
from datetime import datetime, timedelta

conn = sqlite3.connect("sector_wave.db", check_same_thread=False)
cursor = conn.cursor()

# ------------------------------
# Helper functions
# ------------------------------
def get_ticker_id(symbol: str):
    cursor.execute("SELECT id FROM tickers WHERE symbol=?", (symbol,))
    res = cursor.fetchone()
    return res[0] if res else None

def fetch_prices(tickers_list, start_date, end_date):
    all_data = {}
    batch_size = 10
    for i in range(0, len(tickers_list), batch_size):
        batch = tickers_list[i:i+batch_size]
        tk = Ticker(batch)
        hist = tk.history(start=start_date, end=end_date, interval="1d")
        if isinstance(hist.index, pd.MultiIndex):
            for ticker in batch:
                ticker_data = hist.xs(ticker, level='symbol', drop_level=False)
                if not ticker_data.empty:
                    df = ticker_data.reset_index()
                    df['symbol'] = ticker
                    all_data[ticker] = df
        else:
            if not hist.empty:
                df = hist.reset_index()
                df['symbol'] = batch[0]
                all_data[batch[0]] = df
    return all_data

def normalize_series(series: pd.Series) -> pd.Series:
    if series.isna().all() or len(series) == 0 or series.iloc[0] == 0:
        return pd.Series(index=series.index, dtype=float)
    return (series / series.iloc[0] - 1) * 100

def calculate_metrics(leader_prices: pd.Series, follower_prices: pd.DataFrame, roc_window=14):
    leader_norm = normalize_series(leader_prices)
    if follower_prices.empty:
        followers_avg = pd.Series(0, index=leader_norm.index)
    else:
        followers_norm = pd.DataFrame({col: normalize_series(follower_prices[col]) 
                                      for col in follower_prices.columns})
        followers_avg = followers_norm.mean(axis=1)
    neg_space = leader_norm - followers_avg
    roc_neg_space = neg_space.pct_change(periods=roc_window) * 100
    acc_neg_space = roc_neg_space.diff(roc_window)

    phases = []
    for i in range(len(neg_space)):
        ns = neg_space.iloc[i]
        r = roc_neg_space.iloc[i] if i >= roc_window else np.nan
        a = acc_neg_space.iloc[i] if i >= 2*roc_window else np.nan
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
    return leader_norm, followers_avg, neg_space, roc_neg_space, acc_neg_space, phases

# ------------------------------
# Fetch prices for all tickers
# ------------------------------
all_tickers = [row[0] for row in cursor.execute("SELECT symbol FROM tickers").fetchall()]
end_date = datetime.today()
start_date = end_date - timedelta(days=365)
price_data = fetch_prices(all_tickers, start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d'))

# Insert price data
for symbol, df in price_data.items():
    ticker_id = get_ticker_id(symbol)
    if not ticker_id:
        continue
    for _, row in df.iterrows():
        date_str = row['date'].strftime('%Y-%m-%d') if isinstance(row['date'], pd.Timestamp) else row['date']
        cursor.execute("""
        INSERT OR REPLACE INTO price_data (ticker_id, date, adj_close)
        VALUES (?, ?, ?)
        """, (ticker_id, date_str, float(row.get('adjclose', row.get('close', 0)))))
conn.commit()
print("✅ Prices inserted/updated successfully")

conn.close()

# ==============================
# Sector Wave Dashboard - Part 3: Streamlit Dashboard
# ==============================

import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

st.set_page_config(page_title="Sector Wave Dashboard", page_icon="📊", layout="wide")
st.title("📊 Sector Wave Dashboard")

conn = sqlite3.connect("sector_wave.db", check_same_thread=False)

# Sidebar: Select Sector
sectors_df = pd.read_sql("SELECT id, name FROM sectors", conn)
sector_id = st.sidebar.selectbox("Select Sector", options=sectors_df['id'], 
                                 format_func=lambda x: sectors_df.loc[sectors_df['id']==x,'name'].iloc[0])

# Sidebar: Select Subsector
subsectors_df = pd.read_sql(f"SELECT id, name FROM subsectors WHERE sector_id = {sector_id}", conn)
if not subsectors_df.empty:
    subsector_id = st.sidebar.selectbox("Select Subsector", options=subsectors_df['id'], 
                                       format_func=lambda x: subsectors_df.loc[subsectors_df['id']==x,'name'].iloc[0])
    
    leader_df = pd.read_sql(f"""
    SELECT t.id, t.symbol FROM subsector_tickers st
    JOIN tickers t ON st.ticker_id = t.id
    WHERE st.subsector_id = {subsector_id} AND st.is_leader = 1
    """, conn)
    
    if not leader_df.empty:
        leader_id = leader_df['id'].iloc[0]
        leader_symbol = leader_df['symbol'].iloc[0]
        date_range = st.sidebar.date_input(
            "Select Date Range",
            value=[datetime.now() - timedelta(days=180), datetime.now()],
            max_value=datetime.now()
        )
        start_date = date_range[0].strftime('%Y-%m-%d')
        end_date = date_range[1].strftime('%Y-%m-%d') if len(date_range)>1 else datetime.now().strftime('%Y-%m-%d')
        
        metrics_df = pd.read_sql(f"""
        SELECT date, leader_norm, followers_avg_norm, negative_space, 
               roc_neg_space, acc_neg_space, phase
        FROM sector_metrics
        WHERE subsector_id = {subsector_id} AND leader_id = {leader_id}
          AND date BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY date
        """, conn)
        
        if not metrics_df.empty:
            metrics_df['date'] = pd.to_datetime(metrics_df['date'])
            metrics_df = metrics_df.set_index('date')
            st.subheader("Normalized Price Comparison")
            st.line_chart(metrics_df[['leader_norm','followers_avg_norm']])
            
            st.subheader("Negative Space & ROC")
            st.line_chart(metrics_df[['negative_space','roc_neg_space']])
            
            st.subheader("Current Phase")
            st.info(f"**{metrics_df['phase'].iloc[-1]}**")
            
            st.subheader("Phase Distribution")
            st.bar_chart(metrics_df['phase'].value_counts())
            
            csv = metrics_df.to_csv()
            st.download_button("Download CSV", csv, file_name=f"{leader_symbol}_metrics.csv", mime="text/csv")
        else:
            st.warning("No metrics data for selected range")
    else:
        st.error("No leader defined for this subsector")
else:
    st.warning("No subsectors defined for this sector")

conn.close()
