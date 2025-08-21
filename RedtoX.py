import streamlit as st
import feedparser
import requests
import pandas as pd
import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import os

# ------------------------------
# Page config
# ------------------------------
st.set_page_config(
    page_title="Investment Forum Dashboard",
    page_icon="📊",
    layout="wide"
)

# ------------------------------
# Supabase
# ------------------------------
SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"

# HuggingFace token from env
HF_TOKEN = os.getenv("HF_TOKEN")
if not HF_TOKEN:
    st.warning("HF_TOKEN not found in environment. Classification will not work.")

# --------------------
# OAuth1.0a credentials for X API
# --------------------

API_KEY = "QXjPpeI0S84PbjTfrUYdtvXVV"
API_SECRET = "HMEmAYaDDj6WoakCxJHjRweuLNiSrnN3smaHFepCfvgKJecHkO"
ACCESS_TOKEN = "ztADVMLHe3scH9prv3jr8SyLf"
ACCESS_TOKEN_SECRET = "4aA1mgyD5IodZIc3G8d6tVrGaUsL9BPuXYNhBCOKJNMbw1MptQ"

from requests_oauthlib import OAuth1

# ------------------------------
# Channels and Keywords
# ------------------------------
CHANNEL_KEYWORDS = {
    "investingforbeginners": ["index funds","diversification","long-term growth","dollar-cost averaging","low-cost ETFs"],
    "stocks": ["individual stocks","company analysis","growth investing","earnings reports","sector trends"],
    "RobinHood": ["user-friendly","commission-free","beginner trades","mobile investing","retail focus"],
    "Bogleheads": ["passive investing","index funds","low fees","broad diversification","buy and hold"],
    "SecurityAnalysis": ["fundamental analysis","intrinsic value","margin of safety","financial statements","value investing"],
    "ValueInvesting": ["undervalued stocks","discounted cash flow","long-term","qualitative analysis","contrarian investing"],
    "dividendinvesting": ["dividend growth","income investing","cash flow","dividend yield","reinvestment"],
    "SmallCapInvesting": ["small caps","growth potential","higher risk","emerging companies","market inefficiencies"],
    "investing": ["broader market","mixed strategies","fundamentals","technicals","portfolio management"],
    "StockMarket": ["market news","stock trends","daily updates","technical analysis","trading sentiment"],
    "pennystocks": ["high risk","speculation","microcap stocks","volatility","momentum trading"]
}

# ------------------------------
# Supabase client
# ------------------------------
class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.url = url
        self.headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json"
        }

    def insert_data(self, table: str, data: Dict) -> bool:
        # duplicate prevention based on channel+title
        query = f"channel=eq.{data['channel']}&title=eq.{data['title']}"
        check = requests.get(f"{self.url}/rest/v1/{table}?{query}", headers=self.headers)
        if check.status_code == 200 and check.json():
            return False
        resp = requests.post(f"{self.url}/rest/v1/{table}", headers=self.headers, json=data)
        return resp.status_code in (200, 201)

    def select_data(self, table: str, limit=50):
        resp = requests.get(
            f"{self.url}/rest/v1/{table}?order=created_at.desc&limit={limit}",
            headers=self.headers
        )
        return resp.json() if resp.status_code == 200 else []

# ------------------------------
# Utility functions
# ------------------------------
def strip_html_tags(text):
    return re.sub('<.*?>','', text or "")

def clean_text(text):
    t = strip_html_tags(text)
    t = re.sub(r'\s+',' ',t)
    t = re.sub(r'[^\w\s.,!?-]', '',t)
    return t.strip()

def get_keywords_for_channel(c):
    return CHANNEL_KEYWORDS.get(c.replace("r/","").strip(),["investing","stocks"])

def classify_text(text,keywords,token):
    if not token: return None
    url="https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
    headers={"Authorization":f"Bearer {token}"}
    payload={"inputs":text[:512],"parameters":{"candidate_labels":keywords}}
    r=requests.post(url,headers=headers,json=payload)
    if r.status_code==200:
        js=r.json()
        if js["scores"][0]>0.25:
            return {"label":js["labels"][0],"score":js["scores"][0]}
    return None

def fetch_reddit(channel,limit=20):
    items=[]
    feed=feedparser.parse(f"https://www.reddit.com/r/{channel}.rss")
    for e in feed.entries[:limit]:
        title=clean_text(getattr(e,"title",""))
        summary=clean_text(getattr(e,"summary",""))
        content=clean_text(getattr(e,"content", [{}])[0].get("value","") if hasattr(e,"content") else "")
        text=f"{title} {summary} {content}".strip()
        if text:
            items.append({"channel":channel,"title":title,"full_text":text,"link":getattr(e,"link","")})
    return items


# ------------------------------
# OAuth1 posting helper
# ------------------------------
def post_to_x_oauth1(text: str) -> Dict:
    """Post a tweet using OAuth1 user authentication."""
    url = "https://api.twitter.com/2/tweets"
    auth = OAuth1(API_KEY, API_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    payload = {"text": text[:280]}  # 280-char limit
    try:
        resp = requests.post(url, auth=auth, json=payload)
        if resp.status_code == 201:
            return {"status": "success", "code": 201, "message": resp.json()}
        return {"status": "failed", "code": resp.status_code, "message": resp.text}
    except Exception as e:
        return {"status": "error", "code": None, "message": str(e)}

# ------------------------------
# Dashboard Page
# ------------------------------
def dashboard_page():
    st.header("Investment Forum RSS → NLP → Supabase Dashboard")

    col1, col2 = st.columns(2)
    with col1:
        channels_input = st.text_area("Reddit channels (comma-separated)", "investingforbeginners, stocks, ValueInvesting", height=120)
        max_items = st.slider("Max items per channel", 5, 50, 20)
        fetch_button = st.button("🔄 Fetch & Insert")
    with col2:
        channels = [c.strip() for c in channels_input.split(",") if c.strip()]
        for ch in channels[:5]:
            st.write(f"**r/{ch}:** {', '.join(get_keywords_for_channel(ch))}")
        if len(channels) > 5:
            st.write(f"... and {len(channels)-5} more")

    if fetch_button:
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        log = st.empty()
        total_processed = 0
        total_inserted = 0

        for ch in channels:
            items = fetch_reddit(ch, max_items)
            kw = get_keywords_for_channel(ch)
            for item in items:
                total_processed += 1
                cls = classify_text(item["full_text"], kw, HF_TOKEN)
                if not cls:
                    continue
                row = {
                    "channel": item["channel"],
                    "title": item["title"][:500],
                    "content": item["full_text"][:1000],
                    "classification": cls["label"],
                    "confidence": cls["score"],
                    "keywords_used": ", ".join(kw),
                    "link": item["link"],
                    "created_at": datetime.utcnow().isoformat()
                }
                if supabase.insert_data("reddit_filtered_posts", row):
                    total_inserted += 1
                log.text(f"Processed {total_processed} / Inserted {total_inserted}")

        st.success(f"✅ Done. Processed {total_processed}, inserted {total_inserted}")

    # Show latest 50 records
    st.subheader("Latest 50 Filtered Posts")
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    data = supabase.select_data("reddit_filtered_posts", 50)
    if data:
        for row in data:
            with st.expander(row["title"][:80]):
                st.markdown(f"**Label:** {row['classification']}  (Conf: {row['confidence']:.2f})")
                st.markdown(f"{row['content']}")
    else:
        st.info("No records yet.")

# ------------------------------
# Bulk X Posting Page
# ------------------------------
def bulk_posting_page():
    st.header("🐦 Bulk X Posting (OAuth1)")
    drafts_input = st.text_area("Post drafts (one per line)", height=300)
    delay_s = st.slider("Delay between posts (s)", 1, 60, 10)

    if st.button("🚀 Post All"):
        drafts = [d.strip() for d in drafts_input.split("\n") if d.strip()]
        if not drafts:
            st.warning("No drafts supplied.")
            return

        log_area = st.empty()
        progress = st.progress(0)
        logs = []

        for i, d in enumerate(drafts):
            result = post_to_x_oauth1(d)
            logs.append(f"[{i+1}/{len(drafts)}] {result['status']} | code={result['code']} | msg={str(result['message'])[:120]}")
            log_area.text("\n".join(logs[-10:]))
            progress.progress((i+1)/len(drafts))
            time.sleep(delay_s)

        st.success("✅ Bulk posting complete. Check log above for results.")

# ------------------------------
# Main app
# ------------------------------
def main():
    st.title("📊 Investment Forum Dashboard")
    page = st.selectbox("Navigate", ["Dashboard", "Bulk X Posting"])
    if page == "Dashboard":
        dashboard_page()
    else:
        bulk_posting_page()

if __name__ == "__main__":
    main()
