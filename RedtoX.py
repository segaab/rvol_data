import streamlit as st
import feedparser
import requests
import pandas as pd
import time
import re
from datetime import datetime
from typing import List, Dict

# ------------------------------
# Configuration
# ------------------------------
st.set_page_config(
    page_title="Social Media Dashboard",
    page_icon="📊",
    layout="wide"
)

# ------------------------------
# Hardcoded API keys
# ------------------------------
SUPABASE_URL = "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZlX3JvbGUiLCJpYXQiOjE3NTEzNjY3OTQsImV4cCI6MjA2Njk0Mjc5NH0.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
X_BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAMqZ3gEAAAAAwNcDr%2FYHueePFl5mN35XZC%2FBKcI%3DHsgmTrfXb4SvlLnQ0TyjjH6XjU0kpATYq5RcDf6yArxrCFSXM7"

# ------------------------------
# Channels & Keywords
# ------------------------------
CHANNEL_KEYWORDS = {
    "investingforbeginners": ["index funds", "diversification", "long-term growth", "dollar-cost averaging", "low-cost ETFs"],
    "stocks": ["individual stocks", "company analysis", "growth investing", "earnings reports", "sector trends"],
    "RobinHood": ["user-friendly", "commission-free", "beginner trades", "mobile investing", "retail focus"],
    "Bogleheads": ["passive investing", "index funds", "low fees", "broad diversification", "buy and hold"],
    "SecurityAnalysis": ["fundamental analysis", "intrinsic value", "margin of safety", "financial statements", "value investing"],
    "ValueInvesting": ["undervalued stocks", "discounted cash flow", "long-term", "qualitative analysis", "contrarian investing"],
    "dividendinvesting": ["dividend growth", "income investing", "cash flow", "dividend yield", "reinvestment"],
    "SmallCapInvesting": ["small caps", "growth potential", "higher risk", "emerging companies", "market inefficiencies"],
    "investing": ["broader market", "mixed strategies", "fundamentals", "technicals", "portfolio management"],
    "StockMarket": ["market news", "stock trends", "daily updates", "technical analysis", "trading sentiment"],
    "pennystocks": ["high risk", "speculation", "microcap stocks", "volatility", "momentum trading"]
}

# ------------------------------
# Supabase client with duplicate prevention
# ------------------------------
class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.url = url.rstrip('/')
        self.key = key
        self.headers = {
            'apikey': key,
            'Authorization': f'Bearer {key}',
            'Content-Type': 'application/json'
        }

    def insert_data(self, table: str, data: Dict) -> bool:
        try:
            # Check for duplicates using channel + link
            where_clause = f"channel=eq.{data['channel']}&link=eq.{data['link']}"
            check = requests.get(f"{self.url}/rest/v1/{table}?select=*&{where_clause}", headers=self.headers)
            if check.status_code == 200 and check.json():
                return False  # Duplicate found, skip

            response = requests.post(
                f"{self.url}/rest/v1/{table}",
                headers=self.headers,
                json=data
            )
            return response.status_code in [200, 201]
        except Exception as e:
            st.error(f"Supabase insert error: {str(e)}")
            return False

    def select_data(self, table: str, limit: int = 50) -> List[Dict]:
        try:
            response = requests.get(
                f"{self.url}/rest/v1/{table}?select=*&order=created_at.desc&limit={limit}",
                headers=self.headers
            )
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            st.error(f"Supabase select error: {str(e)}")
            return []

# ------------------------------
# Text processing functions
# ------------------------------
def strip_html_tags(text: str) -> str:
    return re.sub('<.*?>', '', text)

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

# ------------------------------
# Reddit RSS
# ------------------------------
def fetch_reddit_rss(channel: str, max_items: int = 20) -> List[Dict]:
    try:
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
    except Exception as e:
        st.error(f"Error fetching RSS for r/{channel}: {str(e)}")
        return []

# ------------------------------
# X (Twitter) Posting
# ------------------------------
def post_to_x(text: str, bearer_token: str) -> bool:
    try:
        url = "https://api.twitter.com/2/tweets"
        headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
        payload = {"text": text[:280]}
        response = requests.post(url, headers=headers, json=payload)
        return response.status_code == 201
    except Exception as e:
        st.error(f"X posting error: {str(e)}")
        return False

# ------------------------------
# Main Streamlit app
# ------------------------------
def main():
    st.title("📊 Investment Forum Dashboard")
    page = st.selectbox("Navigate", ["Dashboard", "Bulk X Posting"])
    if page == "Dashboard":
        dashboard_page()
    else:
        bulk_posting_page()

# ------------------------------
# Dashboard page
# ------------------------------
def dashboard_page():
    st.header("Investment Forum RSS → Supabase Dashboard")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        channels_input = st.text_area("Enter Reddit channels (comma-separated)", value="investingforbeginners, stocks, ValueInvesting", height=120)
        max_items = st.slider("Max items per channel", 5, 50, 20)
        fetch_button = st.button("🔄 Fetch & Filter Posts", type="primary")
    
    with col2:
        channels = [c.strip() for c in channels_input.split(",") if c.strip()]
        for channel in channels[:5]:
            st.write(f"**r/{channel}:** • {', '.join(get_keywords_for_channel(channel))}")
        if len(channels) > 5:
            st.write(f"... and {len(channels) - 5} more channels")
    
    if fetch_button:
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        progress_bar = st.progress(0)
        status_text = st.empty()
        total_processed = 0
        total_inserted = 0
        
        for i, channel in enumerate(channels):
            status_text.text(f"Processing r/{channel}...")
            items = fetch_reddit_rss(channel, max_items)
            inserted_count = 0
            for item in items:
                total_processed += 1
                data = {
                    'channel': item['channel'],
                    'title': item['title'][:500],
                    'content': item['full_text'][:1000],
                    'classification': 'N/A',
                    'confidence': 1.0,
                    'keywords_used': ', '.join(get_keywords_for_channel(channel)),
                    'link': item['link'],
                    'created_at': datetime.now().isoformat()
                }
                if supabase.insert_data('reddit_filtered_posts', data):
                    total_inserted += 1
                    inserted_count += 1
                time.sleep(0.05)
            progress_bar.progress((i+1)/len(channels))
        
        status_text.text(f"✅ Complete! Processed {total_processed}, inserted {total_inserted}")

    st.subheader("📊 Latest 50 Filtered Posts")
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    results = supabase.select_data('reddit_filtered_posts', 50)
    if results:
        df = pd.DataFrame(results)
        display_columns = [col for col in ['channel','title','classification','confidence','created_at','keywords_used'] if col in df.columns]
        st.dataframe(df[display_columns].copy(), use_container_width=True)
    else:
        st.info("No posts available yet.")

# ------------------------------
# Bulk X posting page
# ------------------------------
def bulk_posting_page():
    st.header("🐦 Bulk X (Twitter) Posting")
    drafts_input = st.text_area("Enter your post drafts (one per line)", height=300)
    delay_seconds = st.slider("Delay between posts (seconds)", 1, 60, 10)
    
    if st.button("🚀 Post All to X", type="primary"):
        drafts = [d.strip() for d in drafts_input.split('\n') if d.strip()]
        progress_bar = st.progress(0)
        for i, draft in enumerate(drafts):
            post_to_x(draft, X_BEARER_TOKEN)
            progress_bar.progress((i+1)/len(drafts))
            time.sleep(delay_seconds)

# ------------------------------
# Run the app
# ------------------------------
if __name__ == "__main__":
    main()
