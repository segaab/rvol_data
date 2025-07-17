import pandas as pd
import os
from dotenv import load_dotenv
import pytz
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv(SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def get_2wk_ago_utc_from(ref_date):
    two_weeks_ago = ref_date - timedelta(days=14
    return two_weeks_ago

def debug_rvol_fetch():
    print("=== DEBUGGING RVOL DATA FETCH ===")
    supabase = get_supabase_client()
    ref_date = datetime.now(timezone.utc)
    cutoff = get_2wk_ago_utc_from(ref_date)
    
    print(fReference date: {ref_date})    print(fCutoff date: {cutoff})  
    # Test ticker
    ticker =GC=F 
    # Check whats in the database for this ticker
    print(fn1. Checking raw data for {ticker}:")
    try:
        debug_response = supabase.table("rvol_data).select("ticker, date, rvol").eq(ticker, ticker).limit(5).execute()
        print(f"Sample data: {debug_response.data}")
    except Exception as e:
        print(f"Error: {e})   
    # Test the actual query
    print(f"\n2. Testing the actual query for {ticker}:")
    try:
        response = (
            supabase.table("rvol_data")
            .select("ticker, name, date, rvol)
            .eq(tickericker)
            .gte(date,cutoff.strftime(%Y-%m-%dT%H:%M:%S))
            .lte("date", ref_date.strftime(%Y-%m-%dT%H:%M:%S"))
            .order("date", desc=True)
            .limit(336          .execute()
        )
        print(f"Query returned {len(response.data)} rows")
        if response.data:
            print(f"First row: {response.data0
            print(f"Last row: {response.data-1  except Exception as e:
        print(f"Error: {e})  
    # Test date format in database
    print(f"\n3. Checking date format in database:")
    try:
        sample_response = supabase.table("rvol_data).select("date").limit(1).execute()
        if sample_response.data:
            print(f"Sample date format: {sample_response.data0te']}")
    except Exception as e:
        print(fError: {e}")

def debug_sector_score_fetch():
    print(n=== DEBUGGING SECTOR SCORE DATA FETCH ===")
    supabase = get_supabase_client()
    ref_date = datetime.now(timezone.utc)
    cutoff = get_2wk_ago_utc_from(ref_date)
    
    print(fReference date: {ref_date})    print(fCutoff date: {cutoff})
    # Test sector
    sector =Metals 
    # Check whats in the database for this sector
    print(fn1. Checking raw data for {sector}:")
    try:
        debug_response = supabase.table(sector_score_data).select("sector, date, hour, sector_score").eq(sector, sector).limit(5).execute()
        print(f"Sample data: {debug_response.data}")
    except Exception as e:
        print(f"Error: {e})   
    # Test the actual query
    print(f"\n2. Testing the actual query for {sector}:")
    try:
        response = (
            supabase.table(sector_score_data")
            .select("sector, date, hour, sector_score)
            .eq(sectorector)
            .gte(date,cutoff.strftime(%Y-%m-%dT%H:%M:%S))
            .lte("date", ref_date.strftime(%Y-%m-%dT%H:%M:%S"))
            .order("date", desc=True)
            .limit(336          .execute()
        )
        print(f"Query returned {len(response.data)} rows")
        if response.data:
            print(f"First row: {response.data0
            print(f"Last row: {response.data-1  except Exception as e:
        print(f"Error: {e})  
    # Test date format in database
    print(f"\n3. Checking date format in database:")
    try:
        sample_response = supabase.table(sector_score_data).select("date").limit(1).execute()
        if sample_response.data:
            print(f"Sample date format: {sample_response.data0te']}")
    except Exception as e:
        print(fError: {e})

def debug_date_formats():
    print("\n=== DEBUGGING DATE FORMATS ===")
    supabase = get_supabase_client()
    
    # Check rvol_data date formats
    print("1. Checking rvol_data date formats:")
    try:
        response = supabase.table("rvol_data).select(date.limit(10).execute()
        print("rvol_data dates:")
        for i, row in enumerate(response.data):
            print(f" [object Object]i+1}: {row['date']}")
    except Exception as e:
        print(f"Error: {e}")
    
    # Check sector_score_data date formats
    print("\n2cking sector_score_data date formats:")
    try:
        response = supabase.table(sector_score_data).select(date.limit(10).execute()
        print(sector_score_data dates:")
        for i, row in enumerate(response.data):
            print(f" [object Object]i+1}: {row['date']}")
    except Exception as e:
        print(f"Error: {e})if __name__ == "__main__":
    debug_rvol_fetch()
    debug_sector_score_fetch()
    debug_date_formats() 