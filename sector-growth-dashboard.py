# chunk1_setup_and_hierarchy.py
import os
import streamlit as st
from supabase import create_client, Client
from yahooquery import Ticker
import pandas as pd
import time

st.set_page_config(page_title="Sector Wave - Setup", layout="wide")
st.title("Sector Wave — Setup & Hierarchy")

# Supabase config (env or fallback)
SUPABASE_URL = os.getenv("SUPABASE_URL", "https://dzddytphimhoxeccxqsw.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# -------------------------
# Sector / Subsector / Leader mapping (as provided)
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

# -------------------------
# Full ticker -> company name mapping (from your INSERT block)
# -------------------------
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
    # chunked upsert
    for i in range(0, len(rows), 500):
        supabase.table(table).upsert(rows[i:i+500], on_conflict=on_conflict).execute()

# -------------------------
# Run upsert process
# -------------------------
with st.expander("Upsert full hierarchy (sectors, subsectors, tickers & links)", expanded=True):
    if st.button("Run full upsert"):
        status = st.empty()
        p = st.progress(0)

        # 1) sectors
        sector_rows = []
        for sname in SECTORS.keys():
            sector_rows.append({"name": sname, "symbol": make_sector_symbol(sname)})
        upsert_rows("sectors", sector_rows, ["name"])
        status.text("Sectors upsert complete.")
        p.progress(0.1)
        time.sleep(0.2)

        # 2) subsectors
        # fetch sectors to map ids
        sectors = supabase.table("sectors").select("id,name").execute().data or []
        sector_id_by_name = {s["name"]: s["id"] for s in sectors}
        subsector_rows = []
        for sname, subs in SECTORS.items():
            sid = sector_id_by_name[sname]
            for subname in subs.keys():
                subsector_rows.append({"name": subname, "sector_id": sid})
        upsert_rows("subsectors", subsector_rows, ["name", "sector_id"])
        status.text("Subsectors upsert complete.")
        p.progress(0.25)
        time.sleep(0.2)

        # 3) tickers (with company names)
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

        # 4) subsector_tickers link table (resolve ticker ids)
        tickers = supabase.table("tickers").select("id,symbol,sector_id").execute().data or []
        ticker_id_by_sym = {t["symbol"]: t["id"] for t in tickers}
        subsectors = supabase.table("subsectors").select("id,name,sector_id").execute().data or []
        subsector_id_by_key = {(s["name"], s["sector_id"]): s["id"] for s in subsectors}

        sublink_rows = []
        for sname, subs in SECTORS.items():
            sid = sector_id_by_name[sname]
            for subname, tlist in subs.items():
                subid = subsector_id_by_key.get((subname, sid))
                leader_sym = LEADERS.get(sname, {}).get(subname, None)
                if not subid:
                    continue
                for sym in tlist:
                    tid = ticker_id_by_sym.get(sym)
                    if not tid:
                        continue
                    sublink_rows.append({
                        "subsector_id": subid,
                        "ticker_id": tid,
                        "is_leader": True if leader_sym and sym == leader_sym else False
                    })

        upsert_rows("subsector_tickers", sublink_rows, ["subsector_id", "ticker_id"])
        p.progress(0.9)
        status.text("Subsector <-> ticker links upserted.")
        time.sleep(0.2)
        p.progress(1.0)
        status.text("✅ Full hierarchy done.")
        st.success("All sectors/subsectors/tickers & links upserted.")

# chunk2_enrich_names.py
import os
import streamlit as st
from supabase import create_client, Client
from yahooquery import Ticker
import pandas as pd

st.subheader("Enrich ticker names (optional)")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://dzddytphimhoxeccxqsw.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

if st.button("Fetch & update missing company names from Yahoo"):
    tickers = supabase.table("tickers").select("symbol,name").execute().data or []
    missing = [t["symbol"] for t in tickers if (not t.get("name") or t.get("name")==t["symbol"])]
    if not missing:
        st.info("No missing names found.")
    else:
        batch = 20
        prog = st.progress(0)
        for i in range(0, len(missing), batch):
            syms = missing[i:i+batch]
            tk = Ticker(syms)
            price_info = tk.price
            updates = []
            for sym in syms:
                meta = price_info.get(sym) or {}
                longname = meta.get("longName") or meta.get("shortName") or sym
                updates.append({"symbol": sym, "name": longname})
            if updates:
                supabase.table("tickers").upsert(updates, on_conflict=["symbol"]).execute()
            prog.progress(min(1.0, (i+batch)/len(missing)))
        st.success("Names enriched from Yahoo.")

# chunk3_fetch_prices.py
import os
import streamlit as st
from supabase import create_client, Client
from yahooquery import Ticker
import pandas as pd
from datetime import datetime, timedelta
import math

st.subheader("Fetch 1 year prices → proce_data_an")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://dzddytphimhoxeccxqsw.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# date range: last 365 days (inclusive)
end_date = datetime.utcnow().date()
start_date = end_date - timedelta(days=365)
st.write(f"Fetching daily history from {start_date} to {end_date}")

tickers = supabase.table("tickers").select("id,symbol").execute().data or []
if not tickers:
    st.error("No tickers present in 'tickers' table. Run chunk1 first.")
else:
    id_by_symbol = {t["symbol"]: t["id"] for t in tickers}
    symbols = list(id_by_symbol.keys())

    batch = 12
    total_batches = math.ceil(len(symbols)/batch)
    prog = st.progress(0)
    status = st.empty()

    def upsert_price_rows(rows):
        # rows: list of {"ticker_id","date","adj_close"}
        for i in range(0, len(rows), 500):
            supabase.table("proce_data_an").upsert(rows[i:i+500], on_conflict=["ticker_id","date"]).execute()

    for b in range(0, len(symbols), batch):
        chunk = symbols[b:b+batch]
        status.text(f"Fetching batch {b//batch +1}/{total_batches}: {', '.join(chunk)}")
        try:
            tk = Ticker(chunk)
            hist = tk.history(start=start_date, end=end_date, interval="1d")
            # handle MultiIndex result
            if isinstance(hist.index, pd.MultiIndex):
                for sym in chunk:
                    try:
                        df = hist.xs(sym, level="symbol", drop_level=False).reset_index()
                        if df.empty:
                            continue
                        # prefer adjusted close if present
                        if "adjclose" in df.columns:
                            adjcol = "adjclose"
                        elif "adjClose" in df.columns:
                            adjcol = "adjClose"
                        else:
                            adjcol = "close"
                        rows = []
                        for _, r in df.iterrows():
                            date = r["date"].date() if hasattr(r["date"], "date") else r["date"]
                            val = r.get(adjcol, None)
                            if pd.isna(val):
                                continue
                            rows.append({"ticker_id": id_by_symbol[sym], "date": date.strftime("%Y-%m-%d"), "adj_close": float(val)})
                        if rows:
                            upsert_price_rows(rows)
                    except Exception:
                        continue
            else:
                # single ticker case (rare for batch>1)
                df = hist.reset_index()
                if not df.empty:
                    sym = chunk[0]
                    if "adjclose" in df.columns:
                        adjcol = "adjclose"
                    elif "adjClose" in df.columns:
                        adjcol = "adjClose"
                    else:
                        adjcol = "close"
                    rows = []
                    for _, r in df.iterrows():
                        date = r["date"].date() if hasattr(r["date"], "date") else r["date"]
                        val = r.get(adjcol, None)
                        if pd.isna(val): continue
                        rows.append({"ticker_id": id_by_symbol[sym], "date": date.strftime("%Y-%m-%d"), "adj_close": float(val)})
                    if rows:
                        upsert_price_rows(rows)
        except Exception as e:
            st.warning(f"Batch fetch error: {e}")
        prog.progress(min(1.0, (b+batch)/len(symbols)))
    status.text("✅ Price fetch & upsert complete.")
    st.success("All prices upserted into proce_data_an.")

# chunk4_compute_metrics.py
import os
import streamlit as st
from supabase import create_client, Client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

st.subheader("Compute Metrics & Upsert")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://dzddytphimhoxeccxqsw.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# helpers
def normalize(series: pd.Series) -> pd.Series:
    if series.empty or series.isna().all():
        return pd.Series(index=series.index, dtype=float)
    first = series.dropna().iloc[0]
    if first == 0:
        return pd.Series(index=series.index, dtype=float)
    return (series / first - 1) * 100.0

def calculate_metrics(leader_prices: pd.Series, follower_prices: pd.DataFrame, roc_window=14):
    leader_norm = normalize(leader_prices)
    if follower_prices is None or follower_prices.empty:
        followers_avg = pd.Series(0, index=leader_norm.index)
    else:
        followers_norm = pd.DataFrame({c: normalize(follower_prices[c]) for c in follower_prices.columns})
        followers_avg = followers_norm.mean(axis=1)
    neg_space = leader_norm - followers_avg
    roc_neg = neg_space.pct_change(periods=roc_window) * 100.0
    acc_neg = roc_neg.diff(roc_window)
    phases = []
    for i in range(len(neg_space)):
        ns = neg_space.iloc[i]
        r = roc_neg.iloc[i] if i >= roc_window else np.nan
        a = acc_neg.iloc[i] if i >= 2 * roc_window else np.nan
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
    return leader_norm, followers_avg, neg_space, roc_neg, acc_neg, phases

def read_price_series(ticker_id: int, start_date: str, end_date: str) -> pd.Series:
    resp = (supabase.table("proce_data_an")
            .select("date,adj_close")
            .eq("ticker_id", ticker_id)
            .gte("date", start_date)
            .lte("date", end_date)
            .order("date", ascending=True)
            .execute())
    df = pd.DataFrame(resp.data) if resp.data else pd.DataFrame(columns=["date","adj_close"])
    if df.empty:
        return pd.Series(dtype=float)
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date").sort_index()
    return df["adj_close"]

# get leaders (subsector_tickers with is_leader=true) with joins to tickers/subsectors
leaders_resp = (supabase.table("subsector_tickers")
                .select("subsector_id,is_leader,tickers!inner(id,symbol),subsectors!inner(id,name,sector_id)")
                .eq("is_leader", True)
                .execute())
leaders = leaders_resp.data or []
if not leaders:
    st.warning("No leaders found in subsector_tickers (is_leader=true). Run chunk1 first.")
else:
    start = (datetime.utcnow().date() - timedelta(days=365)).strftime("%Y-%m-%d")
    end = datetime.utcnow().date().strftime("%Y-%m-%d")
    prog = st.progress(0)
    rows_total = 0
    for i, r in enumerate(leaders):
        subsector_id = r["subsectors"]["id"]
        leader_id = r["tickers"]["id"]
        leader_sym = r["tickers"]["symbol"]
        st.write(f"Processing subsector {subsector_id} (leader {leader_sym})")

        leader_series = read_price_series(leader_id, start, end)
        if leader_series.empty:
            st.write(f"No leader prices for {leader_sym}, skipping.")
            prog.progress((i+1)/len(leaders))
            continue

        # get followers for subsector
        followers_resp = (supabase.table("subsector_tickers")
                          .select("ticker_id,tickers!inner(id,symbol)")
                          .eq("subsector_id", subsector_id)
                          .eq("is_leader", False)
                          .execute())
        followers = followers_resp.data or []
        follower_prices = pd.DataFrame(index=leader_series.index)
        for f in followers:
            fid = f["ticker_id"]
            sym = f["tickers"]["symbol"]
            series = read_price_series(fid, start, end)
            if not series.empty:
                series = series.reindex(leader_series.index)
                follower_prices[sym] = series

        follower_prices = follower_prices.dropna(axis=1, how="all")
        if follower_prices.empty:
            st.write(f"No valid follower prices for subsector {subsector_id}, skipping.")
            prog.progress((i+1)/len(leaders))
            continue

        leader_norm, followers_avg, neg_space, roc_neg, acc_neg, phases = calculate_metrics(leader_series, follower_prices, roc_window=14)

        mdf = pd.DataFrame({
            "date": leader_norm.index,
            "leader_norm": leader_norm.values,
            "followers_avg_norm": followers_avg.values,
            "negative_space": neg_space.values,
            "roc_neg_space": roc_neg.values,
            "acc_neg_space": acc_neg.values,
            "phase": phases
        })
        # prepare upsert rows
        rows = []
        for _, x in mdf.iterrows():
            rows.append({
                "sector_id": r["subsectors"]["sector_id"],
                "subsector_id": subsector_id,
                "leader_id": leader_id,
                "date": x["date"].strftime("%Y-%m-%d"),
                "roc_window": 14,
                "leader_norm": float(x["leader_norm"] if not pd.isna(x["leader_norm"]) else None),
                "followers_avg_norm": float(x["followers_avg_norm"] if not pd.isna(x["followers_avg_norm"]) else None),
                "negative_space": float(x["negative_space"] if not pd.isna(x["negative_space"]) else None),
                "roc_neg_space": float(x["roc_neg_space"]) if not pd.isna(x["roc_neg_space"]) else None,
                "acc_neg_space": float(x["acc_neg_space"]) if not pd.isna(x["acc_neg_space"]) else None,
                "phase": x["phase"]
            })
        # upsert in batches
        for j in range(0, len(rows), 500):
            supabase.table("sector_metrics").upsert(rows[j:j+500],
                                                   on_conflict=["subsector_id","leader_id","date","roc_window"]).execute()
        rows_total += len(rows)
        prog.progress((i+1)/len(leaders))
    st.success(f"Finished metrics upsert — rows upserted: approx {rows_total}")
# chunk5_dashboard.py
import os
import streamlit as st
import pandas as pd
from supabase import create_client, Client
from datetime import datetime, timedelta

st.set_page_config(page_title="Sector Wave Dashboard", layout="wide")
st.title("Sector Wave Dashboard — Explorer")

SUPABASE_URL = os.getenv("SUPABASE_URL", "https://dzddytphimhoxeccxqsw.supabase.co")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY",
    "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
)
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# side controls
sectors = supabase.table("sectors").select("id,name").order("name").execute().data or []
if not sectors:
    st.warning("No sectors found. Run the setup (chunk 1).")
    st.stop()
sectors_df = pd.DataFrame(sectors)
sector_id = st.sidebar.selectbox("Sector", options=sectors_df["id"].tolist(),
                                format_func=lambda x: sectors_df.loc[sectors_df["id"]==x,"name"].iloc[0])

subsecs = supabase.table("subsectors").select("id,name").eq("sector_id", sector_id).order("name").execute().data or []
if not subsecs:
    st.warning("No subsectors for this sector.")
    st.stop()
subsecs_df = pd.DataFrame(subsecs)
subsector_id = st.sidebar.selectbox("Subsector", options=subsecs_df["id"].tolist(),
                                   format_func=lambda x: subsecs_df.loc[subsecs_df["id"]==x,"name"].iloc[0])

# find leader
leader_resp = supabase.table("subsector_tickers").select("ticker_id,is_leader,tickers!inner(id,symbol,name)").eq("subsector_id", subsector_id).eq("is_leader", True).execute().data or []
if not leader_resp:
    st.error("No leader configured for subsector.")
    st.stop()
leader = leader_resp[0]["tickers"]
leader_id = leader["id"]
leader_symbol = leader["symbol"]

# date range
default_end = datetime.utcnow()
default_start = default_end - timedelta(days=180)
date_range = st.sidebar.date_input("Date range", value=[default_start.date(), default_end.date()], max_value=default_end.date())
start_date = date_range[0].strftime("%Y-%m-%d")
end_date = date_range[1].strftime("%Y-%m-%d")

# fetch metrics
metrics = (supabase.table("sector_metrics")
           .select("date,leader_norm,followers_avg_norm,negative_space,roc_neg_space,acc_neg_space,phase")
           .eq("subsector_id", subsector_id)
           .eq("leader_id", leader_id)
           .gte("date", start_date)
           .lte("date", end_date)
           .order("date", ascending=True)
           .execute().data) or []

if not metrics:
    st.warning("No metrics for selection (run chunk 4 to compute).")
    st.stop()

df = pd.DataFrame(metrics)
df["date"] = pd.to_datetime(df["date"])
df = df.set_index("date")

st.subheader(f"Leader: {leader_symbol} — Normalized vs Followers' avg")
st.line_chart(df[["leader_norm","followers_avg_norm"]])

st.subheader("Negative Space & ROC")
st.line_chart(df[["negative_space","roc_neg_space"]])

latest_phase = df["phase"].iloc[-1]
st.subheader("Current Phase")
st.info(f"**{latest_phase}**")

st.subheader("Phase Distribution")
st.bar_chart(df["phase"].value_counts())

csv = df.to_csv()
st.download_button("Download CSV", data=csv, file_name=f"{leader_symbol}_metrics.csv", mime="text/csv")

