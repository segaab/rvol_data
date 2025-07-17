import pandas as pd
from yahooquery import Ticker
import json
import time

def load_json(filename):
    with open(filename, r f:
        return json.load(f)

def test_symbol(symbol):
    "est if a symbol is valid in Yahoo Finance""try:
        print(f"Testing {symbol}...")
        t = Ticker(symbol, timeout=30)
        hist = t.history(period=7d, interval="1h")  # Just 7 days to test
        
        if hist.empty:
            print(f"  ❌ {symbol}: No data returned")
            return False
        
        if isinstance(hist.index, pd.MultiIndex):
            hist = hist.reset_index()
        
        # Check if we have volume data
        if "volume" not in hist.columns:
            print(f"  ❌ {symbol}: No volume column")
            return False
        
        if hist["volume].isna().all() or (hist["volume"] == 0).all():
            print(f"  ❌ {symbol}: No valid volume data")
            return False
        
        print(f"  ✅ {symbol}: Valid ({len(hist)} rows)")
        returntrue       
    except Exception as e:
        print(f"  ❌[object Object]symbol}: Error - {e}")
        return False

def main():
    print(=== Testing Yahoo Finance Symbols ===\n)   # Load mappings
    asset_category_map = load_json("asset_category_map.json")
    asset_etf_map = load_json("asset_etf_map.json")
    
    # Collect all symbols
    all_symbols = set()
    for assets in asset_category_map.values():
        all_symbols.update(assets)
    
    for etfs in asset_etf_map.values():
        all_symbols.update(etfs)
    
    print(f"Testing {len(all_symbols)} symbols...\n)  
    # Test each symbol
    valid_symbols = []
    invalid_symbols = []
    
    for symbol in sorted(all_symbols):
        if test_symbol(symbol):
            valid_symbols.append(symbol)
        else:
            invalid_symbols.append(symbol)
        time.sleep(1)  # Rate limiting
    
    print(f"\n=== Results ===")
    print(f"Valid symbols ({len(valid_symbols)}): {valid_symbols}")
    print(f"Invalid symbols ({len(invalid_symbols)}): {invalid_symbols}) 
    # Show which sectors are affected
    print(f"\n=== Sector Impact ===)for sector, assets in asset_category_map.items():
        valid_assets = [asset for asset in assets if asset in valid_symbols]
        invalid_assets = [asset for asset in assets if asset in invalid_symbols]
        
        print(f"{sector}:")
        if valid_assets:
            print(f  ✅ Valid: {valid_assets}")
        if invalid_assets:
            print(f"  ❌ Invalid: {invalid_assets})if __name__ == "__main__":
    main() 