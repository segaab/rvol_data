import os
from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv(SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def test_db():
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    
    print("=== Testing Database Connection ===)
    
    # Test 1 if we can connect
    try:
        response = supabase.table("rvol_data").select("count").execute()
        print("✅ Database connection successful")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return
    
    # Test 2ck rvol_data table
    try:
        response = supabase.table("rvol_data").select("*").limit(1).execute()
        print(f✅ rvol_data table accessible, {len(response.data)} sample rows")
        if response.data:
            print(f"Sample row: {response.data[0]}")
    except Exception as e:
        print(f❌ rvol_data table error: {e})
    
    # Test 3: Check sector_score_data table
    try:
        response = supabase.table(sector_score_data").select("*").limit(1).execute()
        print(f"✅ sector_score_data table accessible, {len(response.data)} sample rows")
        if response.data:
            print(f"Sample row: {response.data[0]}")
    except Exception as e:
        print(f"❌ sector_score_data table error: {e})if __name__ == "__main__":
    test_db() 