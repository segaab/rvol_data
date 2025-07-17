import pandas as pd
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from supabase import create_client

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv(SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def get_supabase_client():
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def debug_database():
    print(===DEBUGGING DATABASE ===")
    supabase = get_supabase_client()
    
    # Check rvol_data
    print("\n1. Checking rvol_data table:")
    try:
        response = supabase.table("rvol_data").select("*").limit(3).execute()
        print(f"Total rows returned: {len(response.data)}")
        if response.data:
            print("Sample data:)
            for i, row in enumerate(response.data):
                print(f  Row {i+1}: {row}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Check sector_score_data
    print("\n2cking sector_score_data table:")
    try:
        response = supabase.table(sector_score_data").select("*").limit(3).execute()
        print(f"Total rows returned: {len(response.data)}")
        if response.data:
            print("Sample data:)
            for i, row in enumerate(response.data):
                print(f  Row {i+1}: {row}")
    except Exception as e:
        print(f"Error: {e})   # Test specific ticker query
    print("\n3. Testing specific ticker query (GC=F):")
    try:
        response = supabase.table("rvol_data).select("ticker, date, rvol").eq(ticker,GC=F).limit(5).execute()
        print(f"GC=F rows returned: {len(response.data)}")
        if response.data:
            print("Sample GC=F data:)
            for i, row in enumerate(response.data):
                print(f  Row {i+1}: {row}")
    except Exception as e:
        print(f"Error: {e})   # Test specific sector query
    print("\n4. Testing specific sector query (Metals):")
    try:
        response = supabase.table(sector_score_data).select("sector, date, hour, sector_score").eq("sector,Metals").limit(5).execute()
        print(f"Metals rows returned: {len(response.data)}")
        if response.data:
            print("Sample Metals data:)
            for i, row in enumerate(response.data):
                print(f  Row {i+1}: {row}")
    except Exception as e:
        print(f"Error: {e})if __name__ == "__main__":
    debug_database() 