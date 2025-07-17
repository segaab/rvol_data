import pandas as pd
from supabase._sync.client import create_client
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Supabase credentials
SUPABASE_URL = os.getenv("SUPABASE_URL") or "https://dzddytphimhoxeccxqsw.supabase.co"
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImR6ZGR5dHBoaW1ob3hlY2N4cXN3Iiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1MTM2Njc5NCwiZXhwIjoyMDY2OTQyNzk0fQ.ng0ST7-V-cDBD0Jc80_0DFWXylzE-gte2I9MCX7qb0Q"

def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def main():
    supabase = get_supabase_client()
    
    print("=== Checking rvol_data table ===")
    try:
        response = supabase.table("rvol_data").select("*").limit(5).execute()
        print(f"Total rows in rvol_data: {len(response.data)}")
        if response.data:
            print("Sample data:")
            for i, row in enumerate(response.data[:3]):
                print(f"  Row {i+1}: {row}")
        else:
            print("No data found in rvol_data table")
    except Exception as e:
        print(f"Error accessing rvol_data: {e}")
    
    print("\n=== Checking sector_score_data table ===")
    try:
        response = supabase.table("sector_score_data").select("*").limit(5).execute()
        print(f"Total rows in sector_score_data: {len(response.data)}")
        if response.data:
            print("Sample data:")
            for i, row in enumerate(response.data[:3]):
                print(f"  Row {i+1}: {row}")
        else:
            print("No data found in sector_score_data table")
    except Exception as e:
        print(f"Error accessing sector_score_data: {e}")
    
    print("\n=== Checking unique tickers in rvol_data ===")
    try:
        response = supabase.table("rvol_data").select("ticker").execute()
        if response.data:
            df = pd.DataFrame(response.data)
            unique_tickers = df['ticker'].unique()
            print(f"Unique tickers: {sorted(unique_tickers)}")
        else:
            print("No tickers found")
    except Exception as e:
        print(f"Error checking tickers: {e}")

if __name__ == "__main__":
    main() 