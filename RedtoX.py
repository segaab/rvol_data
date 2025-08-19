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

# Configuration
st.set_page_config(
    page_title="Social Media Dashboard",
    page_icon="📊",
    layout="wide"
)

# Initialize session state
if 'supabase_url' not in st.session_state:
    st.session_state.supabase_url = ""
if 'supabase_key' not in st.session_state:
    st.session_state.supabase_key = ""
if 'hf_token' not in st.session_state:
    st.session_state.hf_token = ""
if 'x_bearer_token' not in st.session_state:
    st.session_state.x_bearer_token = ""

# Channel-specific keywords for investment models
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

# Default channels for easy selection
DEFAULT_CHANNELS = [
    "investingforbeginners", "stocks", "RobinHood", "Bogleheads", "SecurityAnalysis",
    "ValueInvesting", "dividendinvesting", "SmallCapInvesting", "investing", 
    "StockMarket", "pennystocks"
]

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
                f"{self.url}/rest/v1/{table}?order=created_at.desc&limit={limit}",
                headers=self.headers
            )
            if response.status_code == 200:
                return response.json()
            return []
        except Exception as e:
            st.error(f"Supabase select error: {str(e)}")
            return []

def strip_html_tags(text: str) -> str:
    """Remove HTML/XML tags from text"""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def clean_text(text: str) -> str:
    """Clean and normalize text"""
    if not text:
        return ""
    # Remove HTML tags
    text = strip_html_tags(text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?-]', '', text)
    return text.strip()

def get_keywords_for_channel(channel: str) -> List[str]:
    """Get specific keywords for a channel, with fallback to general keywords"""
    # Remove 'r/' prefix if present
    clean_channel = channel.replace('r/', '').strip()
    
    if clean_channel in CHANNEL_KEYWORDS:
        return CHANNEL_KEYWORDS[clean_channel]
    else:
        # Fallback to general investment keywords
        return ["investing", "stocks", "financial", "market", "portfolio"]

def classify_text_with_huggingface(text: str, keywords: List[str], hf_token: str) -> Optional[Dict]:
    """Classify text using HuggingFace zero-shot classification"""
    try:
        # Using Facebook's BART model for zero-shot classification
        api_url = "https://api-inference.huggingface.co/models/facebook/bart-large-mnli"
        headers = {"Authorization": f"Bearer {hf_token}"}
        
        payload = {
            "inputs": text[:512],  # Limit text length
            "parameters": {
                "candidate_labels": keywords
            }
        }
        
        response = requests.post(api_url, headers=headers, json=payload)
        
        if response.status_code == 200:
            result = response.json()
            if isinstance(result, dict) and 'labels' in result and 'scores' in result:
                # Return the top classification if confidence > 0.25 (lowered for investment terms)
                if result['scores'][0] > 0.25:
                    return {
                        'label': result['labels'][0],
                        'score': result['scores'][0]
                    }
        return None
    except Exception as e:
        st.warning(f"HuggingFace API error: {str(e)}")
        return None

def fetch_reddit_rss(channel: str, max_items: int = 20) -> List[Dict]:
    """Fetch and parse Reddit RSS feed"""
    try:
        url = f"https://www.reddit.com/r/{channel}.rss"
        feed = feedparser.parse(url)
        
        items = []
        for entry in feed.entries[:max_items]:
            # Extract and clean text
            title = clean_text(getattr(entry, 'title', ''))
            summary = clean_text(getattr(entry, 'summary', ''))
            content = clean_text(getattr(entry, 'content', [{}])[0].get('value', '') if hasattr(entry, 'content') else '')
            
            # Combine all text
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

def post_to_x(text: str, bearer_token: str) -> bool:
    """Post to X (Twitter) using API v2"""
    try:
        url = "https://api.twitter.com/2/tweets"
        headers = {
            "Authorization": f"Bearer {bearer_token}",
            "Content-Type": "application/json"
        }
        payload = {"text": text[:280]}  # Limit to 280 characters
        
        response = requests.post(url, headers=headers, json=payload)
        return response.status_code == 201
    except Exception as e:
        st.error(f"X posting error: {str(e)}")
        return False

def main():
    st.title("📊 Investment Forum Dashboard")
    
    # Sidebar configuration
    st.sidebar.header("Configuration")
    
    # API Keys configuration
    with st.sidebar.expander("API Keys", expanded=False):
        st.session_state.supabase_url = st.text_input(
            "Supabase URL", 
            value=st.session_state.supabase_url,
            type="password"
        )
        st.session_state.supabase_key = st.text_input(
            "Supabase Service Key", 
            value=st.session_state.supabase_key,
            type="password"
        )
        st.session_state.hf_token = st.text_input(
            "HuggingFace Token", 
            value=st.session_state.hf_token,
            type="password"
        )
        st.session_state.x_bearer_token = st.text_input(
            "X (Twitter) Bearer Token", 
            value=st.session_state.x_bearer_token,
            type="password"
        )
    
    # Display channel keywords reference
    with st.sidebar.expander("📋 Channel Keywords Reference", expanded=False):
        st.write("**Investment Model Keywords by Channel:**")
        for channel, keywords in CHANNEL_KEYWORDS.items():
            st.write(f"**r/{channel}:**")
            st.write(f"• {', '.join(keywords)}")
            st.write("")
    
    # Navigation
    page = st.sidebar.selectbox(
        "Navigate",
        ["Dashboard", "Bulk X Posting"]
    )
    
    if page == "Dashboard":
        dashboard_page()
    else:
        bulk_posting_page()

def dashboard_page():
    st.header("Investment Forum RSS → NLP → Supabase Dashboard")
    
    # Create two columns
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("Reddit Investment Channels")
        
        # Quick select buttons for predefined channels
        st.write("**Quick Select:**")
        col_a, col_b, col_c = st.columns(3)
        
        with col_a:
            if st.button("Beginner Channels"):
                selected = "investingforbeginners, stocks, RobinHood, Bogleheads"
                st.session_state.channels_input = selected
        
        with col_b:
            if st.button("Intermediate Channels"):
                selected = "SecurityAnalysis, ValueInvesting, dividendinvesting, SmallCapInvesting"
                st.session_state.channels_input = selected
        
        with col_c:
            if st.button("All Channels"):
                selected = ", ".join(DEFAULT_CHANNELS)
                st.session_state.channels_input = selected
        
        # Text area for manual input
        channels_input = st.text_area(
            "Enter Reddit channels (comma-separated)",
            value=getattr(st.session_state, 'channels_input', "investingforbeginners, stocks, ValueInvesting"),
            height=120,
            help="Channels will use their specific investment model keywords for classification"
        )
        
        max_items = st.slider("Max items per channel", 5, 50, 20)
        
        fetch_button = st.button("🔄 Fetch & Filter Posts", type="primary")
    
    with col2:
        st.subheader("Configuration Status")
        
        # Check configuration
        config_status = {
            "Supabase URL": bool(st.session_state.supabase_url),
            "Supabase Key": bool(st.session_state.supabase_key),
            "HuggingFace Token": bool(st.session_state.hf_token)
        }
        
        for key, status in config_status.items():
            st.write(f"{key}: {'✅' if status else '❌'}")
        
        # Show selected channels and their keywords
        if channels_input:
            st.subheader("Selected Channels & Keywords")
            channels = [c.strip() for c in channels_input.split(",") if c.strip()]
            
            for channel in channels[:5]:  # Show first 5 to avoid clutter
                keywords = get_keywords_for_channel(channel)
                st.write(f"**r/{channel}:**")
                st.write(f"• {', '.join(keywords)}")
            
            if len(channels) > 5:
                st.write(f"... and {len(channels) - 5} more channels")
    
    # Process feeds when button is clicked
    if fetch_button:
        if not all([st.session_state.supabase_url, st.session_state.supabase_key, st.session_state.hf_token]):
            st.error("Please configure all required API keys in the sidebar.")
            return
        
        # Parse inputs
        channels = [c.strip() for c in channels_input.split(",") if c.strip()]
        
        if not channels:
            st.error("Please enter at least one Reddit channel.")
            return
        
        # Initialize Supabase client
        supabase = SupabaseClient(st.session_state.supabase_url, st.session_state.supabase_key)
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        total_processed = 0
        total_filtered = 0
        
        # Results tracking
        results_summary = {}
        
        for i, channel in enumerate(channels):
            status_text.text(f"Processing r/{channel}...")
            
            # Get channel-specific keywords
            keywords = get_keywords_for_channel(channel)
            
            # Fetch RSS data
            items = fetch_reddit_rss(channel, max_items)
            
            channel_filtered = 0
            
            for item in items:
                total_processed += 1
                
                # Classify with HuggingFace using channel-specific keywords
                classification = classify_text_with_huggingface(
                    item['full_text'], 
                    keywords, 
                    st.session_state.hf_token
                )
                
                if classification:
                    # Prepare data for Supabase
                    data = {
                        'channel': item['channel'],
                        'title': item['title'][:500],  # Limit length
                        'content': item['full_text'][:1000],  # Limit length
                        'classification': classification['label'],
                        'confidence': classification['score'],
                        'keywords_used': ', '.join(keywords),  # Store the keywords used
                        'link': item['link'],
                        'created_at': datetime.now().isoformat()
                    }
                    
                    # Insert to Supabase
                    if supabase.insert_data('reddit_filtered_posts', data):
                        total_filtered += 1
                        channel_filtered += 1
                
                # Small delay to avoid rate limits
                time.sleep(0.1)
            
            results_summary[channel] = {
                'processed': len(items),
                'filtered': channel_filtered,
                'keywords': keywords
            }
            
            progress_bar.progress((i + 1) / len(channels))
        
        # Display results summary
        status_text.text(f"✅ Complete! Processed {total_processed} posts, filtered {total_filtered}")
        
        st.subheader("📈 Processing Summary")
        summary_data = []
        for channel, stats in results_summary.items():
            summary_data.append({
                'Channel': f"r/{channel}",
                'Posts Processed': stats['processed'],
                'Posts Filtered': stats['filtered'],
                'Filter Rate': f"{(stats['filtered']/stats['processed']*100):.1f}%" if stats['processed'] > 0 else "0%",
                'Keywords Used': ', '.join(stats['keywords'])
            })
        
        if summary_data:
            st.dataframe(pd.DataFrame(summary_data), use_container_width=True)
        
        # Auto-refresh the results
        st.rerun()
    
    # Display recent results
    st.subheader("📊 Latest 50 Filtered Posts")
    
    if st.session_state.supabase_url and st.session_state.supabase_key:
        supabase = SupabaseClient(st.session_state.supabase_url, st.session_state.supabase_key)
        results = supabase.select_data('reddit_filtered_posts', 50)
        
        if results:
            df = pd.DataFrame(results)
            # Format the dataframe for better display
            if not df.empty:
                display_columns = ['channel', 'title', 'classification', 'confidence', 'created_at']
                if 'keywords_used' in df.columns:
                    display_columns.insert(-1, 'keywords_used')
                
                display_df = df[display_columns].copy()
                display_df['confidence'] = display_df['confidence'].round(3)
                st.dataframe(display_df, use_container_width=True)
                
                # Filter by channel
                channels_in_results = sorted(df['channel'].unique())
                selected_channel = st.selectbox("Filter by channel:", ['All'] + channels_in_results)
                
                if selected_channel != 'All':
                    filtered_df = df[df['channel'] == selected_channel]
                    st.write(f"**Posts from r/{selected_channel}:**")
                    st.dataframe(filtered_df[display_columns], use_container_width=True)
                
                # Download button
                csv = df.to_csv(index=False)
                st.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"investment_posts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
        else:
            st.info("No filtered posts found. Try fetching some data first!")
    else:
        st.warning("Configure Supabase credentials to view stored posts.")

def bulk_posting_page():
    st.header("🐦 Bulk X (Twitter) Posting")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("Post Drafts")
        drafts_input = st.text_area(
            "Enter your post drafts (one per line)",
            height=300,
            placeholder="Enter each post on a new line...\n\nExample:\nJust discovered an amazing investment strategy! 📈\nDiversification is key to long-term wealth building 💰\nValue investing principles never go out of style 📊"
        )
        
        delay_seconds = st.slider(
            "Delay between posts (seconds)", 
            min_value=1, 
            max_value=60, 
            value=10,
            help="Delay to avoid rate limits"
        )
    
    with col2:
        st.subheader("Configuration")
        
        # Check X token
        x_configured = bool(st.session_state.x_bearer_token)
        st.write(f"X API Token: {'✅' if x_configured else '❌'}")
        
        if not x_configured:
            st.warning("Configure X Bearer Token in the sidebar.")
        
        # Preview
        if drafts_input:
            drafts = [d.strip() for d in drafts_input.split('\n') if d.strip()]
            st.write(f"**Posts to send:** {len(drafts)}")
            
            if drafts:
                st.write("**Preview:**")
                for i, draft in enumerate(drafts[:3], 1):
                    st.write(f"{i}. {draft[:50]}...")
                if len(drafts) > 3:
                    st.write(f"... and {len(drafts) - 3} more")
    
    # Post button
    if st.button("🚀 Post All to X", type="primary"):
        if not st.session_state.x_bearer_token:
            st.error("Please configure X Bearer Token in the sidebar.")
            return
        
        if not drafts_input.strip():
            st.error("Please enter some post drafts.")
            return
        
        drafts = [d.strip() for d in drafts_input.split('\n') if d.strip()]
        
        if not drafts:
            st.error("No valid drafts found.")
            return
        
        # Confirm before posting
        if st.button(f"⚠️ Confirm: Post {len(drafts)} drafts to X?", type="secondary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            successful_posts = 0
            failed_posts = 0
            
            for i, draft in enumerate(drafts):
                status_text.text(f"Posting {i+1}/{len(drafts)}: {draft[:50]}...")
                
                success = post_to_x(draft, st.session_state.x_bearer_token)
                
                if success:
                    successful_posts += 1
                    st.success(f"✅ Posted: {draft[:50]}...")
                else:
                    failed_posts += 1

if __name__ == "__main__":
    main()
    
