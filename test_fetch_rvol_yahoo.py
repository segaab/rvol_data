import pandas as pd
from yahooquery import Ticker
from datetime import timedelta

# Ticker and ETF maps (from update_rvol.py)
TICKER_MAP = {
    "GOLD - COMMODITY EXCHANGE INC.": "GC=F",
    "EURO FX - CHICAGO MERCANTILE EXCHANGE": "6E=F",
    "AUSTRALIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6A=F",
    "BITCOIN - CHICAGO MERCANTILE EXCHANGE": "BTC-USD",
    "MICRO BITCOIN - CHICAGO MERCANTILE EXCHANGE": "MBT=F",
    "MICRO ETHER - CHICAGO MERCANTILE EXCHANGE": "ETH-USD",
    "SILVER - COMMODITY EXCHANGE INC.": "SI=F",
    "WTI FINANCIAL CRUDE OIL - NEW YORK MERCANTILE EXCHANGE": "CL=F",
    "JAPANESE YEN - CHICAGO MERCANTILE EXCHANGE": "6J=F",
    "CANADIAN DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6C=F",
    "BRITISH POUND - CHICAGO MERCANTILE EXCHANGE": "6B=F",
    "U.S. DOLLAR INDEX - ICE FUTURES U.S.": "DX-Y.NYB",
    "NEW ZEALAND DOLLAR - CHICAGO MERCANTILE EXCHANGE": "6N=F",
    "SWISS FRANC - CHICAGO MERCANTILE EXCHANGE": "6S=F",
    "DOW JONES U.S. REAL ESTATE IDX - CHICAGO BOARD OF TRADE": "^DJI",
    "E-MINI S&P 500 STOCK INDEX - CHICAGO MERCANTILE EXCHANGE": "ES=F",
    "NASDAQ-100 STOCK INDEX (MINI) - CHICAGO MERCANTILE EXCHANGE": "NQ=F",
    "NIKKEI STOCK AVERAGE - CHICAGO MERCANTILE EXCHANGE": "^N225",
    "SPDR S&P 500 ETF TRUST": "SPY"
}
ETF_MAP = {
    "GC=F": ("GLD", "SPDR Gold Trust"),
    "SI=F": ("SLV", "iShares Silver Trust"),
    "CL=F": ("USO", "United States Oil Fund"),
    "6E=F": ("FXE", "Invesco CurrencyShares Euro"),
    "6A=F": ("FXA", "Invesco CurrencyShares AUD"),
    "6J=F": ("FXY", "Invesco CurrencyShares JPY"),
    "6C=F": ("FXC", "Invesco CurrencyShares CAD"),
    "6B=F": ("FXB", "Invesco CurrencyShares GBP"),
    "6N=F": ("FXA", "Invesco CurrencyShares AUD"),
    "6S=F": ("FXF", "Invesco CurrencyShares CHF"),
    "DX-Y.NYB": ("UUP", "Invesco DB US Dollar Bullish"),
    "BTC-USD": ("BITO", "ProShares Bitcoin Strategy ETF"),
    "MBT=F": ("BITO", "ProShares Bitcoin Strategy ETF"),
    "ETH-USD": ("ETHE", "Grayscale Ethereum Trust"),
    "^DJI": ("IYR", "iShares U.S. Real Estate ETF"),
    "ES=F": ("SPY", "SPDR S&P 500 ETF Trust"),
    "NQ=F": ("QQQ", "Invesco QQQ Trust"),
    "^N225": ("EWJ", "iShares MSCI Japan ETF"),
    "SPY": ("SPY", "SPDR S&P 500 ETF Trust"),
}

DAYS = 730  # 2 years
ROLLING_WINDOW = 120

# All unique asset and ETF symbols
symbols = list(set(TICKER_MAP.values()) | set(v[0] for v in ETF_MAP.values()))

for symbol in symbols:
    print(f"\n[INFO] Fetching last {DAYS} days of 1-hour data for {symbol}...")
    t = Ticker(symbol, timeout=60)
    hist = t.history(period=f"{DAYS}d", interval="1h")
    if isinstance(hist, pd.DataFrame) and not hist.empty:
        if isinstance(hist.index, pd.MultiIndex):
            hist = hist.reset_index()
        hist = hist.rename(columns={"symbol": "ticker"})
        hist = hist.dropna(subset=["volume", "date"])
        hist = hist[hist["volume"] > 0]
        hist["datetime"] = pd.to_datetime(hist["date"], errors="coerce", utc=True)
        hist = hist.dropna(subset=["datetime"])
        hist = hist.sort_values("datetime")
        # Convert to GMT+3
        hist["datetime_gmt3"] = hist["datetime"] + timedelta(hours=3)
        hist["datetime_gmt3"] = hist["datetime_gmt3"].dt.strftime("%Y-%m-%dT%H:%M:%S+03:00")
        # Calculate avg_volume and rvol
        hist["avg_volume"] = hist["volume"].rolling(ROLLING_WINDOW).mean()
        hist["rvol"] = hist["volume"] / hist["avg_volume"]
        print(hist[["datetime_gmt3", "volume", "avg_volume", "rvol"]].tail(10))
    else:
        print(f"[WARN] No data found for {symbol}.") 