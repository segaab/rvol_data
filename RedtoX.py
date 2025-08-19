aimport streamlit as st
import feedparser
import requests
import pandas as pd
import time
import re
from datetime import datetime
from typing import List, Dict, Optional
import urllib.parse
import os

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
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"
X_BEARER_TOKEN = "AAAAAAAAAAAAAAAAAAAAAMqZ3gEAAAAAwNcDr%2FYHueePFl5mN35XZC%2FBKcI%3DHsgmTrfXb4SvlLnQ0TyjjH6XjU0kpATYq5RcDf6yArxrCFSXM7"
HF_TOKEN = os.getenv("HF_TOKEN")

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
# Supabase client with logging & duplicate prevention
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

    def insert_data(self, table: str, data: Dict) -> str:
        try:
            where_clause = f"link=eq.{urllib.parse.quote(data['link'])}"
            check = requests.get(f"{self.url}/rest/v1/{table}?select=*&{where_clause}", headers=self.headers)
            if check.status_code == 200 and check.json():
                return "duplicate"

            response = requests.post(f"{self.url}/rest/v1/{table}", headers=self.headers, json=data)
            if response.status_code in [200, 201]:
                return "inserted"
            st.warning(f"❌ Insert failed ({response.status_code}): {response.text} | Data: {str(data)[:200]}")
            return "error"
        except requests.exceptions.RequestException as e:
            st.error(f"⚠ Requests exception: {str(e)} | Data: {str(data)[:200]}")
            return "error"
        except Exception as e:
            st.error(f"⚠ Unexpected exception: {str(e)} | Data: {str(data)[:200]}")
            return "error"

    def select_data(self, table: str, limit: int = 50) -> List[Dict]:
        try:
            response = requests.get(f"{self.url}/rest/v1/{table}?select=*&order=created_at.desc&limit={limit}", headers=self.headers)
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            st.error(f"Supabase select error: {str(e)}")
            return []

# ------------------------------
# Text processing & classification
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
    return CHANNEL_KEYWORDS.get(channel.replace('r/','').strip(), ["investing","stocks","financial","market","portfolio"])

def classify_text_hf(text: str, keywords: List[str], hf_token: str) -> Optional[Dict]:
    if not hf_token:
        return None
    try:
        api_url = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
        headers = {"Authorization": f"Bearer {hf_token}"}
        payload = {"inputs": text[:512], "parameters": {"candidate_labels": keywords}}
        resp = requests.post(api_url, headers=headers, json=payload)
        if resp.status_code == 200:
            result = resp.json()
            if result['scores'][0] > 0.25:
                return {"label": result['labels'][0], "score": result['scores'][0]}
    except Exception as e:
        st.warning(f"HuggingFace error: {str(e)}")
    return None

# ------------------------------
# Reddit RSS fetching
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
# X Posting
# ------------------------------
def post_to_x(text: str, bearer_token: str) -> bool:
    try:
        url = "https://api.twitter.com/2/tweets"
        headers = {"Authorization": f"Bearer {bearer_token}", "Content-Type": "application/json"}
        payload = {"text": text[:280]}
        resp = requests.post(url, headers=headers, json=payload)
        if resp.status_code == 201:
            return True
        st.error(f"X posting failed | Status: {resp.status_code} | Response: {resp.text}")
        return False
    except Exception as e:
        st.error(f"X posting exception: {str(e)}")
        return False

# ------------------------------
# Dashboard page
# ------------------------------
def dashboard_page():
    st.header("Investment Forum RSS → NLP → Supabase Dashboard")
    
    col1, col2 = st.columns([1,1])
    with col1:
        channels_input = st.text_area("Reddit channels (comma-separated)", value="investingforbeginners, stocks, ValueInvesting", height=120)
        max_items = st.slider("Max items per channel", 5, 50, 20)
        fetch_button = st.button("🔄 Fetch & Insert Posts")
    
    with col2:
        channels = [c.strip() for c in channels_input.split(",") if c.strip()]
        for channel in channels[:5]:
            st.write(f"**r/{channel}:** • {', '.join(get_keywords_for_channel(channel))}")
        if len(channels) > 5:
            st.write(f"... and {len(channels) - 5} more channels")
    
    if fetch_button:
        if not HF_TOKEN:
            st.error("HuggingFace token not set.")
            return
        
        supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        log_box = st.empty()
        total_processed = 0
        total_inserted = 0
        all_logs = []

        for channel in channels:
            items = fetch_reddit_rss(channel, max_items)
            keywords = get_keywords_for_channel(channel)
            for item in items:
                total_processed += 1
                classification = classify_text_hf(item['full_text'], keywords, HF_TOKEN)
                data = {
                    'channel': item['channel'],
                    'title': item['title'][:500],
                    'content': item['full_text'][:1000],
                    'classification': classification['label'] if classification else "N/A",
                    'confidence': classification['score'] if classification else 1.0,
                    'keywords_used': ', '.join(keywords),
                    'link': item['link'],
                    'created_at': datetime.now().isoformat()
                }
                status = supabase.insert_data('reddit_filtered_posts', data)
                if status == "inserted":
                    total_inserted += 1
                    log_msg = f"✅ Inserted: {data['title'][:50]}..."
                elif status == "duplicate":
                    log_msg = f"⚠ Skipped (duplicate): {data['title'][:50]}..."
                else:
                    log_msg = f"❌ Insert error: {data['title'][:50]} | Status={status}"
                all_logs.append(log_msg)
                log_box.text("\n".join(all_logs[-10:]))
                time.sleep(0.05)

        st.success(f"✅ Complete! Processed {total_processed}, inserted {total_inserted}")

    # ------------------------------
    # Display latest 50 posts with expanders
    # ------------------------------
    st.subheader("📊 Latest 50 Filtered Posts")
    supabase = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
    results = supabase.select_data('reddit_filtered_posts', 50)

    if results:
        df = pd.DataFrame(results)
        for col in ['title','content','classification','confidence']:
            if col not in df.columns:
                df[col] = ""
        
        df = df.sort_values(by='confidence', ascending=False).reset_index(drop=True)

        for idx, row in df.iterrows():
            with st.expander(f"{row['title'][:80]}"):
                st.markdown(f"**Top Tag:** {row['classification']} (Confidence: {row['confidence']:.2f})")
                st.markdown(f"**Content:**\n{row['content']}")
    else:
        st.info("No posts found.")

# ------------------------------
# Bulk X Posting page with detailed logging
# ------------------------------
def bulk_posting_page():
    st.header("🐦 Bulk X Posting with Detailed Logs")
    drafts_input = st.text_area("Enter one post per line", height=300)
    delay_seconds = st.slider("Delay between posts (seconds)", 1, 60, 10)
    
    if st.button("🚀 Post All"):
        if not X_BEARER_TOKEN:
            st.error("X Bearer Token not set or invalid.")
            return
        
        drafts = [d.strip() for d in drafts_input.split('\n') if d.strip()]
        if not drafts:
            st.warning("No drafts to post.")
            return
        
        progress_bar = st.progress(0)
        log_box = st.empty()
        logs = []

        for i, draft in enumerate(drafts):
            try:
                url = "https://api.twitter.com/2/tweets"
                headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}", "Content-Type": "application/json"}
                payload = {"text": draft[:280]}
                resp = requests.post(url, headers=headers, json=payload)
                
                if resp.status_code == 201:
                    log_msg = f"✅ Posted successfully: {draft[:50]}..."
                else:
                    log_msg = f"❌ Failed | Status: {resp.status_code} | Response: {resp.text[:200]} | Draft: {draft[:50]}..."
                
                logs.append(log_msg)
                log_box.text("\n".join(logs[-10:]))
            except Exception as e:
                log_msg = f"⚠ Exception: {str(e)} | Draft: {draft[:50]}..."
                logs.append(log_msg)
                log_box.text("\n".join(logs[-10:]))
            
            progress_bar.progress((i+1)/len(drafts))
            time.sleep(delay_seconds)
        
        st.success(f"✅ Posting complete. Total drafts: {len(drafts)}")

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
