import os
import streamlit as st
from supabase import create_client, Client
from yahooquery import Ticker
import pandas as pd
import time

st.set_page_config(page_title="Sector Wave - Setup", layout="wide")
st.title("Sector Wave — Setup & Hierarchy")

# -------------------------
# Supabase config (hardcoded)
# -------------------------
SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# -------------------------
# Sector / Subsector / Leader mapping
# -------------------------
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

TICKER_COMPANY = {
    # Technology & AI
    "NVDA":"NVIDIA Corporation","AMD":"Advanced Micro Devices","PLTR":"Palantir Technologies",
    "MSFT":"Microsoft Corporation","GOOG":"Alphabet Inc. Class C","AI":"C3.ai",
    "INTC":"Intel Corporation","TSM":"Taiwan Semiconductor","AMAT":"Applied Materials",
    "MU":"Micron Technology","AMZN":"Amazon.com Inc.","GOOGL":"Alphabet Inc. Class A",
    "CRM":"Salesforce Inc.","NET":"Cloudflare Inc.","SNOW":"Snowflake Inc.",
    "CRWD":"CrowdStrike","PANW":"Palo Alto Networks","ZS":"Zscaler","FTNT":"Fortinet",
    "S":"SentinelOne","GEN":"Gen Digital",
    # Clean Energy & EV
    "TSLA":"Tesla Inc.","RIVN":"Rivian Automotive","LCID":"Lucid Motors","NIO":"NIO Inc.",
    "LI":"Li Auto Inc.","XPEV":"XPeng Inc.","ALB":"Albemarle Corp","LTHM":"Livent Corp",
    "LAC":"Lithium Americas","SQM":"Sociedad Quimica y Minera","PLL":"Piedmont Lithium","FREY":"FREYR Battery",
    "SEDG":"SolarEdge","ENPH":"Enphase Energy","FSLR":"First Solar","SPWR":"SunPower Corp",
    "NOVA":"Sunnova Energy","MAXN":"Maxeon Solar","NEE":"NextEra Energy","DNNGY":"DONG Energy",
    "DQ":"DAQO New Energy","VWDRY":"Vestas Wind Systems","CWEN":"Clearway Energy","BEP":"Brookfield Renewable",
    # Biotech & Healthcare
    "MRNA":"Moderna Inc.","BNTX":"BioNTech SE","CRSP":"CRISPR Therapeutics","EDIT":"Editas Medicine",
    "NTLA":"Intellia Therapeutics","BEAM":"Beam Therapeutics","ISRG":"Intuitive Surgical","MDT":"Medtronic",
    "BSX":"Boston Scientific","EW":"Edwards Lifesciences","ZBH":"Zimmer Biomet","ABMD":"Abiomed",
    "TDOC":"Teladoc Health","DOCS":"Doximity","ONEM":"1Life Healthcare","AMWL":"Amwell","ACCD":"Accolade Inc.","PHR":"Phreesia",
    "DNA":"Ginkgo Bioworks","PACB":"Pacific Biosciences","TWST":"Twist Bioscience","BLI":"Berkeley Lights",
    "TXG":"10x Genomics","ME":"23andMe",
    # Financial Innovation
    "SQ":"Block Inc.","PYPL":"PayPal Holdings","COIN":"Coinbase Global","HOOD":"Robinhood Markets",
    "SOFI":"SoFi Technologies","UPST":"Upstart Holdings","V":"Visa Inc.","MA":"Mastercard Inc.",
    "ADYEY":"Adyen NV","AFRM":"Affirm Holdings","MSTR":"MicroStrategy","MARA":"Marathon Digital",
    "RIOT":"Riot Blockchain","HUT":"Hut 8 Mining","BITF":"Bitfarms","NU":"Nu Holdings","DAVE":"Dave Inc.",
    "PSEC":"Prospect Capital","LC":"LendingClub","OCFT":"OneConnect",
    # Web3 & Metaverse
    "RBLX":"Roblox Corp","U":"Unity Software","TTWO":"Take-Two Interactive","EA":"Electronic Arts",
    "ATVI":"Activision Blizzard","PLTK":"Playtika","META":"Meta Platforms","SNAP":"Snap Inc.",
    "PINS":"Pinterest","MTCH":"Match Group","BMBL":"Bumble Inc.","SI":"Silvergate Capital",
    "MTTR":"Matterport","IMMR":"Immersion Corp","VUZI":"Vuzix",
    # Space & Defense
    "SPCE":"Virgin Galactic","RKLB":"Rocket Lab","ASTR":"Astra Space","MNTS":"Momentus","SATL":"Satellogic","RDW":"Redwire Corp",
    "LMT":"Lockheed Martin","RTX":"Raytheon Technologies","NOC":"Northrop Grumman","GD":"General Dynamics","BA":"Boeing","HII":"Huntington Ingalls",
    "IRDM":"Iridium Comm","GSAT":"Globalstar","MAXR":"Maxar Technologies","VSAT":"ViaSat","SATS":"EchoStar","LLAP":"Terran Orbital",
    "AIR":"AAR Corp","TDG":"TransDigm Group","HEI":"Heico Corp","SPR":"Spirit AeroSystems","AJRD":"Aerojet Rocketdyne",
    # Future Materials
    "DD":"DuPont","CE":"Celanese","PPG":"PPG Industries","CTVA":"Corteva","ECL":"Ecolab",
    "MP":"MP Materials","UUUU":"Energy Fuels","CCJ":"Cameco Corp","DNN":"Denison Mines",
    "NANO":"Nanometrics","CAMT":"Camtek","FORM":"FormFactor","NCTY":"The9 Ltd","WATT":"Energous Corp","NNDM":"Nano Dimension",
    "OC":"Owens Corning","TREX":"Trex Company","AZEK":"Azek Company","JCI":"Johnson Controls","TTEK":"Tetra Tech","CLH":"Clean Harbors",
    # Infrastructure
    "BLDR":"Builders FirstSource","PWR":"Quanta Services","DY":"Dycom Industries","MTZ":"MasTec Inc.",
    "WIRE":"Encore Wire","AGX":"Argan Inc.","COMM":"CommScope","BAND":"Bandwidth Inc.","ERIC":"Ericsson","NOK":"Nokia",
    "AVNW":"Aviat Networks","IDCC":"InterDigital","AEE":"Ameren Corp","DUK":"Duke Energy","SO":"Southern Company","PCG":"PG&E Corp",
    "EIX":"Edison Int'l","PRIM":"Primoris Services","ACM":"AECOM","FLR":"Fluor Corp"
}

# -------------------------
# Helpers
# -------------------------
def make_sector_symbol(sector_name: str) -> str:
    return ''.join([w[0] for w in sector_name.split() if w.lower() not in ["&","and","of","the"]]).upper()

def upsert_rows(table: str, rows: list, on_conflict: list):
    if not rows:
        return
    for i in range(0, len(rows), 500):
        supabase.table(table).upsert(rows[i:i+500], on_conflict=on_conflict).execute()

# -------------------------
# Upsert process
# -------------------------
with st.expander("Upsert full hierarchy (sectors, subsectors, tickers & links)", expanded=True):
    if st.button("Run full upsert"):
        status = st.empty()
        p = st.progress(0)

        # 1) sectors
        sector_rows = [{"name": sname, "symbol": make_sector_symbol(sname)} for sname in SECTORS.keys()]
        upsert_rows("sectors", sector_rows, ["name"])
        status.text("Sectors upsert complete.")
        p.progress(0.1)
        time.sleep(0.2)

        # 2) subsectors
        sectors = supabase.table("sectors").select("id,name").execute().data or []
        sector_id_by_name = {s["name"]: s["id"] for s in sectors}
        subsector_rows = []
        for sname, subs in SECTORS.items():
            sid = sector_id_by_name[sname]
            for subname in subs.keys():
                subsector_rows.append({"name": subname, "sector_id": sid})
        upsert_rows("subsectors", subsector_rows, ["name","sector_id"])
        status.text("Subsectors upsert complete.")
        p.progress(0.25)
        time.sleep(0.2)

        # 3) tickers
        ticker_rows = []
        for sname, subs in SECTORS.items():
            sid = sector_id_by_name[sname]
            for subname, tlist in subs.items():
                for sym in tlist:
                    ticker_rows.append({
                        "symbol": sym,
                        "name": TICKER_COMPANY.get(sym, sym),
                        "sector_id": sid
                    })
        upsert_rows("tickers", ticker_rows, ["symbol"])
        status.text("Tickers upserted (with company names).")
        p.progress(0.5)
        time.sleep(0.2)

        # 4) subsector_tickers links
        tickers = supabase.table("tickers").select("id,symbol,sector_id").execute().data or []
        ticker_id_by_sym = {t["symbol"]: t["id"] for t in tickers}
        subsectors = supabase.table("subsectors").select("id,name,sector_id").execute().data or []
        subsector_id_by_key = {(s["name"], s["sector_id"]): s["id"] for s in subsectors}

        sublink_rows = []
        for sname, subs in SECTORS.items():
            sid = sector_id_by_name[sname]
            for subname, tlist in subs.items():
                subid = subsector_id_by_key.get((subname,sid))
                leader_sym = LEADERS.get(sname, {}).get(subname, None)
                if not subid: continue
                for sym in tlist:
                    tid = ticker_id_by_sym.get(sym)
                    if not tid: continue
                    sublink_rows.append({
                        "subsector_id": subid,
                        "ticker_id": tid,
                        "is_leader": sym == leader_sym
                    })
        upsert_rows("subsector_tickers", sublink_rows, ["subsector_id","ticker_id"])
        p.progress(0.9)
        status.text("Subsector <-> ticker links upserted.")
        time.sleep(0.2)
        p.progress(1.0)
        status.text("✅ Full hierarchy done.")
        st.success("All sectors/subsectors/tickers & links upserted.")

import pandas as pd
import numpy as np
import time
from yahooquery import Ticker
from supabase import create_client, Client
import streamlit as st

# -------------------------
# Supabase config (hardcoded)
# -------------------------
SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

st.title("Sector Wave — Price Data Analysis")

# -------------------------
# Helpers
# -------------------------
def fetch_price_data(tickers: list, period="6mo", interval="1d"):
    """Fetch price data from YahooQuery."""
    if not tickers:
        return pd.DataFrame()
    data = {}
    for sym in tickers:
        try:
            ticker = Ticker(sym)
            hist = ticker.history(period=period, interval=interval)
            if hist.empty: continue
            hist = hist.reset_index()
            hist = hist.rename(columns={"adjclose":"close"})
            data[sym] = hist[["date","close"]].copy()
            time.sleep(0.05)
        except Exception as e:
            st.warning(f"Error fetching {sym}: {e}")
    return data

def compute_price_analysis(price_df: pd.DataFrame):
    """Compute basic analysis for a ticker's price series."""
    if price_df.empty: return {}
    df = price_df.copy()
    df["pct_change"] = df["close"].pct_change()
    analysis = {
        "last_close": df["close"].iloc[-1],
        "mean": df["close"].mean(),
        "std": df["close"].std(),
        "volatility": df["pct_change"].std(),
        "returns": df["pct_change"].sum()
    }
    return analysis

def upsert_price_analysis(price_data_an: dict):
    """Upsert analysis into Supabase."""
    rows = []
    tickers = supabase.table("tickers").select("id,symbol").execute().data or []
    ticker_id_map = {t["symbol"]: t["id"] for t in tickers}
    for sym, df in price_data_an.items():
        analysis = compute_price_analysis(df)
        tid = ticker_id_map.get(sym)
        if not tid: continue
        rows.append({
            "ticker_id": tid,
            "last_close": analysis.get("last_close"),
            "mean": analysis.get("mean"),
            "std": analysis.get("std"),
            "volatility": analysis.get("volatility"),
            "returns": analysis.get("returns")
        })
    if rows:
        supabase.table("price_analysis").upsert(rows, on_conflict=["ticker_id"]).execute()

# -------------------------
# Streamlit UI
# -------------------------
with st.expander("Fetch & analyze ticker prices", expanded=True):
    tickers_input = st.text_area("Tickers (comma-separated)", "NVDA,TSLA,MRNA")
    tickers_list = [t.strip().upper() for t in tickers_input.split(",") if t.strip()]
    if st.button("Fetch & analyze"):
        status = st.empty()
        status.text("Fetching price data...")
        price_data_an = fetch_price_data(tickers_list)
        status.text("Analyzing price data...")
        upsert_price_analysis(price_data_an)
        status.text("✅ Price analysis upserted to Supabase.")
        st.success("Done!")

# ==============================
# Chunk 4: Price Fetching, Processing & Upsert
# ==============================

def fetch_price_data(tickers, start_date, end_date):
    """
    Fetch OHLCV price data for given tickers between start and end dates.
    Returns a DataFrame with cleaned price data.
    """
    try:
        price_data = {}
        for ticker in tickers:
            try:
                t = Ticker(ticker)
                hist = t.history(start=start_date, end=end_date)
                if hist is not None and not hist.empty:
                    hist = hist.reset_index()
                    hist["symbol"] = ticker
                    price_data[ticker] = hist
                    logging.info(f"✅ Successfully fetched data for {ticker}")
                else:
                    logging.warning(f"⚠️ No data returned for {ticker}")
            except Exception as e:
                logging.error(f"❌ Error fetching data for {ticker}: {e}")

        if not price_data:
            logging.error("❌ No price data fetched for any ticker.")
            return pd.DataFrame()

        df = pd.concat(price_data.values(), ignore_index=True)
        df = df.rename(
            columns={
                "date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            }
        )
        df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]
        df["date"] = pd.to_datetime(df["date"]).dt.date

        return df

    except Exception as e:
        logging.critical(f"❌ Critical error in fetch_price_data: {e}")
        return pd.DataFrame()


def upsert_price_data(df):
    """
    Upsert the processed price data into the Supabase `price_data_an` table.
    """
    if df.empty:
        logging.warning("⚠️ No data to upsert into price_data_an.")
        return

    try:
        payload = df.to_dict(orient="records")
        response = supabase.table("price_data_an").upsert(payload).execute()

        if response.get("status_code") not in (200, 201, 204):
            logging.error(
                f"❌ Failed to upsert data into price_data_an. Response: {response}"
            )
        else:
            logging.info(
                f"✅ Successfully upserted {len(payload)} records into price_data_an"
            )

    except Exception as e:
        logging.critical(f"❌ Critical error during upsert to price_data_an: {e}")
# ==============================
# Chunk 4: Price Fetching, Processing & Upsert
# ==============================

def fetch_price_data(tickers, start_date, end_date):
    """
    Fetch OHLCV price data for given tickers between start and end dates.
    Returns a DataFrame with cleaned price data.
    """
    try:
        price_data = {}
        for ticker in tickers:
            try:
                t = Ticker(ticker)
                hist = t.history(start=start_date, end=end_date)
                if hist is not None and not hist.empty:
                    hist = hist.reset_index()
                    hist["symbol"] = ticker
                    price_data[ticker] = hist
                    logging.info(f"✅ Successfully fetched data for {ticker}")
                else:
                    logging.warning(f"⚠️ No data returned for {ticker}")
            except Exception as e:
                logging.error(f"❌ Error fetching data for {ticker}: {e}")

        if not price_data:
            logging.error("❌ No price data fetched for any ticker.")
            return pd.DataFrame()

        df = pd.concat(price_data.values(), ignore_index=True)
        df = df.rename(
            columns={
                "date": "date",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume",
            }
        )
        df = df[["date", "symbol", "open", "high", "low", "close", "volume"]]
        df["date"] = pd.to_datetime(df["date"]).dt.date

        return df

    except Exception as e:
        logging.critical(f"❌ Critical error in fetch_price_data: {e}")
        return pd.DataFrame()


def upsert_price_data(df):
    """
    Upsert the processed price data into the Supabase `price_data_an` table.
    """
    if df.empty:
        logging.warning("⚠️ No data to upsert into price_data_an.")
        return

    try:
        payload = df.to_dict(orient="records")
        response = supabase.table("price_data_an").upsert(payload).execute()

        if response.get("status_code") not in (200, 201, 204):
            logging.error(
                f"❌ Failed to upsert data into price_data_an. Response: {response}"
            )
        else:
            logging.info(
                f"✅ Successfully upserted {len(payload)} records into price_data_an"
            )

    except Exception as e:
        logging.critical(f"❌ Critical error during upsert to price_data_an: {e}")


# ==============================
# Streamlit UI & Dashboard
# ==============================

# Sidebar date filters
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date", datetime.now() - timedelta(days=90))
end_date = st.sidebar.date_input("End Date", datetime.now())
selected_sector = st.sidebar.selectbox("Select Sector", ["All"] + list(set(sector_mapping.values())))

# Fetch price data from Supabase
try:
    query = supabase.table("price_data_an").select("*").gte("date", str(start_date)).lte("date", str(end_date))
    if selected_sector != "All":
        tickers_in_sector = [t for t, s in sector_mapping.items() if s == selected_sector]
        query = query.in_("ticker", tickers_in_sector)

    response = query.execute()
    if response.data:
        df = pd.DataFrame(response.data)
        df["date"] = pd.to_datetime(df["date"])
        df.sort_values(by=["ticker", "date"], inplace=True)

        # Show raw data preview
        st.subheader("Fetched Price Data")
        st.dataframe(df.head(50))

        # Sector performance aggregation
        st.subheader("Sector Performance Overview")
        sector_perf = df.groupby(["date", "ticker"])["close"].last().reset_index()
        sector_perf["sector"] = sector_perf["ticker"].map(sector_mapping)
        sector_summary = sector_perf.groupby(["date", "sector"])["close"].mean().reset_index()

        # Line chart
        fig = px.line(
            sector_summary,
            x="date",
            y="close",
            color="sector",
            title="Sector Average Performance",
        )
        st.plotly_chart(fig, use_container_width=True)

        # Detailed metrics
        st.subheader("Detailed Metrics")
        latest_data = sector_summary.groupby("sector").tail(1).sort_values(by="close", ascending=False)
        st.table(latest_data)

        # CSV download
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="Download Price Data CSV",
            data=csv,
            file_name="price_data_an.csv",
            mime="text/csv",
        )

    else:
        st.warning("No data available for the selected filters.")
except Exception as e:
    logger.error(f"Error fetching data from Supabase for dashboard: {str(e)}")
    st.error("Failed to load dashboard data. Check logs for details.")
                                               

