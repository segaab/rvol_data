import streamlit as st
import feedparser
import requests
import pandas as pd
import json
import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import os
from dotenv import load_dotenv

# --------------------------------------
# Configuration
# --------------------------------------
st.set_page_config(
    page_title="Social Media Dashboard",
    page_icon="📊",
    layout="wide"
)

# Load environment variables (for HuggingFace token)
load_dotenv()

# Hard-coded tokens
st.session_state.supabase_url = "https://dzddytphimhoxeccxsw.supabase.co"
st.session_state.supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSIsInNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
st.session_state.x_bearer_token = "AAAAAAAAAAAAAAAAAAAAAMqZ3gEAAAAAwNcDr%2FYHueePFl5mN35XZC%2FBKcI%3DHsgmTrfXb4SvlLnQ0TyjjH6XjU0kpATYq5RcDf6yArxrCFSXM7"

# HuggingFace token from environment
st.session_state.hf_token = os.getenv("HF_TOKEN", "")

# --------------------------------------
# Keywords and Defaults
# --------------------------------------
CHANNEL_KEYWORDS = {
    "investingforbeginners": ["index funds", "diversification", "long-term growth",
                              "dollar-cost averaging", "low-cost ETFs"],
    "stocks": ["individual stocks", "company analysis", "growth investing",
               "earnings reports", "sector trends"],
    "RobinHood": ["user-friendly", "commission-free", "beginner trades",
                  "mobile investing", "retail focus"],
    "Bogleheads": ["passive investing", "index funds", "low fees",
                   "broad diversification", "buy and hold"],
    "SecurityAnalysis": ["fundamental analysis", "intrinsic value",
                         "margin of safety", "financial statements", "value investing"],
    "ValueInvesting": ["undervalued stocks", "discounted cash flow",
                       "long-term", "qualitative analysis", "contrarian investing"],
    "dividendinvesting": ["dividend growth", "income investing", "cash flow",
                          "dividend yield", "reinvestment"],
    "SmallCapInvesting": ["small caps", "growth potential", "higher risk",
                          "emerging companies", "market inefficiencies"],
    "investing": ["broader market", "mixed strategies", "fundamentals",
                  "technicals", "portfolio management"],
    "StockMarket": ["market news", "stock trends", "daily updates",
                    "technical analysis", "trading sentiment"],
    "pennystocks": ["high risk", "speculation", "microcap stocks",
                    "volatility", "momentum trading"]
}

DEFAULT_CHANNELS = [
    "investingforbeginners", "stocks", "RobinHood", "Bogleheads",
    "SecurityAnalysis", "ValueInvesting", "dividendinvesting",
    "SmallCapInvesting", "investing", "StockMarket", "pennystocks"
]

# --------------------------------------
# Helper Classes/Functions
# --------------------------------------
class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.url = url.rstrip('/')
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json'
        }

    def insert_data(self, table: str, data: Dict) -> bool:
        try:
            resp = requests.post(f"{self.url}/rest/v1/{table}",
                                 headers=self.headers, json=data)
            return resp.status_code in [200, 201]
        except Exception as e:
            st.error(f"Supabase insert error: {str(e)}")
            return False

    def select_data(self, table: str, limit: int = 50) -> List[Dict]:
        try:
            resp = requests.get(
                f"{self.url}/rest/v1/{table}?order=created_at.desc&limit={limit}",
                headers=self.headers
            )
            return resp.json() if resp.status_code == 200 else []
        except Exception as e:
            st.error(f"Supabase select error: {str(e)}")
            return []

def strip_html_tags(text: str) -> str:
    return re.sub(re.compile('<.*?>'), '', text)

def clean_text(text: str) -> str:
    if not text:
        return ""
    text = strip_html_tags(text)
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s.,!?-]', '', text)
    return text.strip()

def get_keywords_for_channel(channel: str) -> List[str]:
    clean_channel = channel.replace('r/', '').strip()
    return CHANNEL_KEYWORDS.get(clean_channel, ["investing", "stocks", "financial", "market", "portfolio"])

def classify_text_with_huggingface(text: str, keywords: List[str], hf_token: str) -> Optional[Dict]:
    try:
        api_url = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
        headers = {"Authorization": f"Bearer {hf_token}"}
        payload = {"inputs": text[:512], "parameters": {"candidate_labels": keywords}}
        resp = requests.post(api_url, headers=headers, json=payload)
        result = resp.json()
        if resp.status_code == 200 and 'scores' in result:
            if result['scores'][0] > 0.25:
                return {"label": result['labels'][0], "score": result['scores'][0]}
        return None
    except:
        return None

def fetch_reddit_rss(channel: str, max_items: int = 20) -> List[Dict]:
    url = f"https://www.reddit.com/r/{channel}.rss"
    feed = feedparser.parse(url)
    items = []
    for entry in feed.entries[:max_items]:
        title = clean_text(getattr(entry, 'title', ''))
        summary = clean_text(getattr(entry, 'summary', ''))
        content = clean_text(getattr(entry, 'content', [{}])[0].get('value', '') if hasattr(entry, 'content') else '')
        full_text = f"{title} {summary} {content}".strip()
        if full_text:
            items.append({
                'channel': channel,
                'title': title,
                'summary': summary,
                'full_text': full_text,
                'link': getattr(entry, 'link', ''),
                'published': getattr(entry, 'published', '')
            })
    return items

def post_to_x(text: str, bearer_token: str) -> bool:
    url = "https://api.twitter.com/2/tweets"
    headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
    resp = requests.post(url, headers=headers, json={"text": text[:280]})
    return resp.status_code == 201

# --------------------------------------
# Streamlit Pages
# --------------------------------------
def dashboard_page():
    st.header("Investment Forum RSS → NLP → Supabase Dashboard")

    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("Reddit Investment Channels")
        col_a, col_b, col_c = st.columns(3)
        with col_a:
            if st.button("Beginner Channels"):
                st.session_state.channels_input = "investingforbeginners, stocks, RobinHood, Bogleheads"
        with col_b:
            if st.button("Intermediate Channels"):
                st.session_state.channels_input = "SecurityAnalysis, ValueInvesting, dividendinvesting, SmallCapInvesting"
        with col_c:
            if st.button("All Channels"):
                st.session_state.channels_input = ", ".join(DEFAULT_CHANNELS)

        channels_input = st.text_area(
            "Enter Reddit channels (comma-separated)",
            value=getattr(st.session_state, 'channels_input', "investingforbeginners, stocks, ValueInvesting"),
            height=120
        )

        max_items = st.slider("Max items per channel", 5, 50, 20)
        fetch_button = st.button("🔄 Fetch & Filter Posts", type="primary")

    with col2:
        st.subheader("Configuration Status")
        config_status = {
            "Supabase URL": bool(st.session_state.supabase_url),
            "Supabase Key": bool(st.session_state.supabase_key),
            "HuggingFace Token": bool(st.session_state.hf_token)
        }
        for key, status in config_status.items():
            st.write(f"{key}: {'✅' if status else '❌'}")

        if channels_input:
            st.subheader("Selected Channels & Keywords")
            channels = [c.strip() for c in channels_input.split(",") if c.strip()]
            for channel in channels[:5]:
                st.write(f"**r/{channel}:** {', '.join(get_keywords_for_channel(channel))}")
            if len(channels) > 5:
                st.write(f"... and {len(channels) - 5} more channels")

    if fetch_button:
        if not all([st.session_state.supabase_url, st.session_state.supabase_key, st.session_state.hf_token]):
            st.error("Please configure all required API values.")
            return

        channels = [c.strip() for c in channels_input.split(",") if c.strip()]
        if not channels:
            st.error("Please enter at least one Reddit channel.")
            return

        supabase = SupabaseClient(st.session_state.supabase_url, st.session_state.supabase_key)
        progress_bar = st.progress(0)
        status_text = st.empty()

        total_processed = 0
        total_filtered = 0
        results_summary = {}

        for i, channel in enumerate(channels):
            status_text.text(f"Processing r/{channel}...")
            keywords = get_keywords_for_channel(channel)
            items = fetch_reddit_rss(channel, max_items)
            filtered_count = 0

            for item in items:
                total_processed += 1
                classification = classify_text_with_huggingface(
                    item['full_text'], keywords, st.session_state.hf_token
                )

                if classification:
                    data = {
                        'channel': item['channel'],
                        'title': item['title'][:500],
                        'content': item['full_text'][:1000],
                        'classification': classification['label'],
                        'confidence': classification['score'],
                        'keywords_used': ', '.join(keywords),
                        'link': item['link'],
                        'created_at': datetime.now().isoformat()
                    }
                    if supabase.insert_data('reddit_filtered_posts', data):
                        total_filtered += 1
                        filtered_count += 1

                time.sleep(0.1)

            results_summary[channel] = {
                'processed': len(items),
                'filtered': filtered_count,
                'keywords': keywords
            }
            progress_bar.progress((i + 1) / len(channels))

        status_text.text(f"✅ Complete! Processed {total_processed} posts, filtered {total_filtered}")
        st.subheader("📈 Processing Summary")
        st.dataframe(pd.DataFrame([
            {
                'Channel': f"r/{ch}",
                'Posts Processed': s['processed'],
                'Posts Filtered': s['filtered'],
                'Filter Rate': f"{(s['filtered']/s['processed']*100):.1f}%" if s['processed'] else "0%",
                'Keywords Used': ', '.join(s['keywords'])
            }
            for ch, s in results_summary.items()
        ]), use_container_width=True)

        st.rerun()

    st.subheader("📊 Latest 50 Filtered Posts")
    if st.session_state.supabase_url and st.session_state.supabase_key:
        supabase = SupabaseClient(st.session_state.supabase_url, st.session_state.supabase_key)
        results = supabase.select_data('reddit_filtered_posts', 50)
        if results:
            df = pd.DataFrame(results)
            display_columns = ['channel', 'title', 'classification', 'confidence', 'keywords_used', 'created_at']
            df['confidence'] = df['confidence'].round(3)
            st.dataframe(df[display_columns].copy(), use_container_width=True)

            channels_in_results = sorted(df['channel'].unique())
            selected_channel = st.selectbox("Filter by channel:", ['All'] + channels_in_results)
            if selected_channel != 'All':
                st.dataframe(df[df['channel'] == selected_channel][display_columns], use_container_width=True)
        else:
            st.info("No filtered posts found. Try fetching some data first!")
    else:
        st.warning("Supabase credentials missing.")

def bulk_posting_page():
    st.header("🐦 Bulk X (Twitter) Posting")
    drafts_input = st.text_area(
        "Enter your post drafts (one per line)",
        height=300,
        placeholder="One post per line..."
    )
    delay_seconds = st.slider("Delay between posts (seconds)", 1, 60, 10)

    x_configured = bool(st.session_state.x_bearer_token)
    st.write(f"X API Token: {'✅' if x_configured else '❌'}")

    if drafts_input:
        drafts = [d.strip() for d in drafts_input.split('\n') if d.strip()]
        st.write(f"**Posts to send:** {len(drafts)}")
        for i, draft in enumerate(drafts[:3], 1):
            st.write(f"{i}. {draft[:50]}...")
        if len(drafts) > 3:
            st.write(f"... and {len(drafts) - 3} more")

    if st.button("🚀 Post All to X", type="primary"):
        if not x_configured or not drafts_input.strip():
            st.error("Please check your X bearer token and drafts.")
            return

        drafts = [d.strip() for d in drafts_input.split('\n') if d.strip()]
        progress_bar = st.progress(0)
        status_text = st.empty()
        sent = 0

        for i, draft in enumerate(drafts):
            status_text.text(f"Posting {i+1}/{len(drafts)}: {draft[:50]}...")
            success = post_to_x(draft, st.session_state.x_bearer_token)
            if success:
                sent += 1
            time.sleep(delay_seconds)
            progress_bar.progress((i+1)/len(drafts))

        st.success(f"✅ Posted {sent} of {len(drafts)} drafts.")

# --------------------------------------
# Run App
# --------------------------------------
def main():
    st.sidebar.header("Navigation")
    page = st.sidebar.selectbox("Page", ["Dashboard", "Bulk X Posting"])
    if page == "Dashboard":
        dashboard_page()
    else:
        bulk_posting_page()

if __name__ == "__main__":
    main()
