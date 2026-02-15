"""
TradersFootprint — Nifty 50 Data Pipeline
==========================================
Fetches real-time data from Yahoo Finance for all Nifty 50 stocks.
Computes technical indicators (RSI, MACD, SMA, EPS growth, margins).
Exports a single JSON file that the website consumes.

SETUP:
  pip install yfinance pandas numpy

USAGE:
  python update_data.py

OUTPUT:
  data/nifty50_data.json  — consumed by the website
"""

import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta

# ============================================
# ALL NIFTY 50 TICKERS (NSE)
# ============================================
NIFTY50_TICKERS = {
    "RELIANCE":    {"ticker": "RELIANCE.NS",    "name": "Reliance Industries",       "sector": "Oil & Gas"},
    "TCS":         {"ticker": "TCS.NS",         "name": "Tata Consultancy Services", "sector": "IT Services"},
    "HDFCBANK":    {"ticker": "HDFCBANK.NS",    "name": "HDFC Bank",                 "sector": "Banking"},
    "INFY":        {"ticker": "INFY.NS",        "name": "Infosys",                   "sector": "IT Services"},
    "ICICIBANK":   {"ticker": "ICICIBANK.NS",   "name": "ICICI Bank",                "sector": "Banking"},
    "BHARTIARTL":  {"ticker": "BHARTIARTL.NS",  "name": "Bharti Airtel",             "sector": "Telecom"},
    "ITC":         {"ticker": "ITC.NS",         "name": "ITC Limited",               "sector": "FMCG"},
    "SBIN":        {"ticker": "SBIN.NS",        "name": "State Bank of India",       "sector": "PSU Banking"},
    "HINDUNILVR":  {"ticker": "HINDUNILVR.NS",  "name": "Hindustan Unilever",        "sector": "FMCG"},
    "LT":          {"ticker": "LT.NS",          "name": "Larsen & Toubro",           "sector": "Infrastructure"},
    "KOTAKBANK":   {"ticker": "KOTAKBANK.NS",   "name": "Kotak Mahindra Bank",       "sector": "Banking"},
    "HCLTECH":     {"ticker": "HCLTECH.NS",     "name": "HCL Technologies",          "sector": "IT Services"},
    "AXISBANK":    {"ticker": "AXISBANK.NS",    "name": "Axis Bank",                 "sector": "Banking"},
    "BAJFINANCE":  {"ticker": "BAJFINANCE.NS",  "name": "Bajaj Finance",             "sector": "NBFC"},
    "MARUTI":      {"ticker": "MARUTI.NS",      "name": "Maruti Suzuki",             "sector": "Automobile"},
    "TITAN":       {"ticker": "TITAN.NS",       "name": "Titan Company",             "sector": "Consumer"},
    "SUNPHARMA":   {"ticker": "SUNPHARMA.NS",   "name": "Sun Pharmaceutical",        "sector": "Pharma"},
    "ASIANPAINT":  {"ticker": "ASIANPAINT.NS",  "name": "Asian Paints",              "sector": "Consumer"},
    "TATAMOTORS":  {"ticker": "TATAMOTORS.NS",  "name": "Tata Motors",               "sector": "Automobile"},
    "WIPRO":       {"ticker": "WIPRO.NS",       "name": "Wipro",                     "sector": "IT Services"},
    "NTPC":        {"ticker": "NTPC.NS",        "name": "NTPC Limited",              "sector": "Power"},
    "POWERGRID":   {"ticker": "POWERGRID.NS",   "name": "Power Grid Corp",           "sector": "Power"},
    "ULTRACEMCO":  {"ticker": "ULTRACEMCO.NS",  "name": "UltraTech Cement",          "sector": "Cement"},
    "ONGC":        {"ticker": "ONGC.NS",        "name": "ONGC",                      "sector": "Oil & Gas"},
    "TATASTEEL":   {"ticker": "TATASTEEL.NS",   "name": "Tata Steel",                "sector": "Metals"},
    "NESTLEIND":   {"ticker": "NESTLEIND.NS",   "name": "Nestle India",              "sector": "FMCG"},
    "BAJAJFINSV":  {"ticker": "BAJAJFINSV.NS",  "name": "Bajaj Finserv",             "sector": "NBFC"},
    "JSWSTEEL":    {"ticker": "JSWSTEEL.NS",    "name": "JSW Steel",                 "sector": "Metals"},
    "ADANIPORTS":  {"ticker": "ADANIPORTS.NS",  "name": "Adani Ports",               "sector": "Infrastructure"},
    "TECHM":       {"ticker": "TECHM.NS",       "name": "Tech Mahindra",             "sector": "IT Services"},
    "COALINDIA":   {"ticker": "COALINDIA.NS",   "name": "Coal India",                "sector": "Mining"},
    "HDFCLIFE":    {"ticker": "HDFCLIFE.NS",    "name": "HDFC Life Insurance",       "sector": "Insurance"},
    "SBILIFE":     {"ticker": "SBILIFE.NS",     "name": "SBI Life Insurance",        "sector": "Insurance"},
    "INDUSINDBK":  {"ticker": "INDUSINDBK.NS",  "name": "IndusInd Bank",             "sector": "Banking"},
    "GRASIM":      {"ticker": "GRASIM.NS",      "name": "Grasim Industries",         "sector": "Cement"},
    "M&M":         {"ticker": "M&M.NS",         "name": "Mahindra & Mahindra",       "sector": "Automobile"},
    "BAJAJ-AUTO":  {"ticker": "BAJAJ-AUTO.NS",  "name": "Bajaj Auto",                "sector": "Automobile"},
    "DRREDDY":     {"ticker": "DRREDDY.NS",     "name": "Dr. Reddy's Labs",          "sector": "Pharma"},
    "CIPLA":       {"ticker": "CIPLA.NS",       "name": "Cipla",                     "sector": "Pharma"},
    "DIVISLAB":    {"ticker": "DIVISLAB.NS",    "name": "Divi's Laboratories",       "sector": "Pharma"},
    "HEROMOTOCO":  {"ticker": "HEROMOTOCO.NS",  "name": "Hero MotoCorp",             "sector": "Automobile"},
    "APOLLOHOSP":  {"ticker": "APOLLOHOSP.NS",  "name": "Apollo Hospitals",          "sector": "Healthcare"},
    "EICHERMOT":   {"ticker": "EICHERMOT.NS",   "name": "Eicher Motors",             "sector": "Automobile"},
    "TATACONSUM":  {"ticker": "TATACONSUM.NS",  "name": "Tata Consumer",             "sector": "FMCG"},
    "BPCL":        {"ticker": "BPCL.NS",        "name": "BPCL",                      "sector": "Oil & Gas"},
    "BRITANNIA":   {"ticker": "BRITANNIA.NS",   "name": "Britannia Industries",      "sector": "FMCG"},
    "HINDALCO":    {"ticker": "HINDALCO.NS",    "name": "Hindalco Industries",       "sector": "Metals"},
    "WIPRO":       {"ticker": "WIPRO.NS",       "name": "Wipro",                     "sector": "IT Services"},
    "ADANIENT":    {"ticker": "ADANIENT.NS",    "name": "Adani Enterprises",         "sector": "Conglomerate"},
    "SHRIRAMFIN":  {"ticker": "SHRIRAMFIN.NS",  "name": "Shriram Finance",           "sector": "NBFC"},
}


# ============================================
# TECHNICAL INDICATOR CALCULATIONS
# ============================================
def compute_rsi(series, period=14):
    """Compute RSI (Relative Strength Index)"""
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def compute_macd(series, fast=12, slow=26, signal=9):
    """Compute MACD, Signal Line, and Histogram"""
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram


def compute_sma(series, period):
    """Compute Simple Moving Average"""
    return series.rolling(window=period, min_periods=1).mean()


def safe_round(val, decimals=2):
    """Safely round a value, return 0 if NaN/None"""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return 0
    return round(float(val), decimals)


def safe_get(info, key, default=0):
    """Safely get a value from yfinance info dict"""
    val = info.get(key, default)
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return default
    return val


# ============================================
# FORMAT MARKET CAP
# ============================================
def format_market_cap(val):
    """Convert market cap to readable Indian format (L Cr, K Cr)"""
    if val >= 1e12:  # Lakh Crore
        return f"{val/1e12:.1f}L Cr"
    elif val >= 1e10:  # Thousand Crore
        return f"{val/1e10:.0f}K Cr"
    elif val >= 1e7:
        return f"{val/1e7:.0f} Cr"
    else:
        return f"{val:,.0f}"


# ============================================
# FETCH & PROCESS SINGLE STOCK
# ============================================
def fetch_stock_data(key, meta):
    """Fetch all data for a single stock from Yahoo Finance"""
    ticker_symbol = meta["ticker"]
    print(f"  Fetching {key} ({ticker_symbol})...")

    try:
        stock = yf.Ticker(ticker_symbol)
        info = stock.info

        # --- PRICE DATA (last 6 months for charts) ---
        hist = stock.history(period="6mo")
        if hist.empty:
            print(f"    WARNING: No price history for {key}, skipping.")
            return None

        close = hist["Close"]
        volume = hist["Volume"]

        # Current price & change
        current_price = safe_round(close.iloc[-1])
        prev_close = safe_round(close.iloc[-2]) if len(close) > 1 else current_price
        change_abs = safe_round(current_price - prev_close)
        change_pct = safe_round((change_abs / prev_close) * 100 if prev_close != 0 else 0)

        # --- FUNDAMENTALS ---
        market_cap = safe_get(info, "marketCap", 0)
        pe = safe_round(safe_get(info, "trailingPE", 0))
        eps = safe_round(safe_get(info, "trailingEps", 0))
        book_value = safe_round(safe_get(info, "bookValue", 0))
        div_yield = safe_round(safe_get(info, "dividendYield", 0) * 100)
        debt_equity = safe_round(safe_get(info, "debtToEquity", 0) / 100 if safe_get(info, "debtToEquity", 0) else 0)
        roe = safe_round(safe_get(info, "returnOnEquity", 0) * 100 if safe_get(info, "returnOnEquity", 0) else 0)
        gross_margin = safe_round(safe_get(info, "grossMargins", 0) * 100)
        operating_margin = safe_round(safe_get(info, "operatingMargins", 0) * 100)
        net_margin = safe_round(safe_get(info, "profitMargins", 0) * 100)
        high_52 = safe_round(safe_get(info, "fiftyTwoWeekHigh", 0))
        low_52 = safe_round(safe_get(info, "fiftyTwoWeekLow", 0))
        revenue = safe_get(info, "totalRevenue", 0)
        net_income = safe_get(info, "netIncomeToCommon", 0)

        # --- QUARTERLY FINANCIALS (for charts) ---
        quarterly_rev = []
        quarterly_profit = []
        quarterly_eps_growth = []
        quarterly_gm = []
        quarterly_om = []
        quarterly_nm = []
        quarter_labels = []

        try:
            q_fin = stock.quarterly_financials
            q_inc = stock.quarterly_income_stmt
            if q_fin is not None and not q_fin.empty:
                cols = q_fin.columns[:5]  # last 5 quarters
                for col in reversed(list(cols)):
                    label = col.strftime("%b'%y")
                    quarter_labels.append(label)

                    rev_val = 0
                    if "Total Revenue" in q_fin.index:
                        rev_val = safe_round(q_fin.loc["Total Revenue", col] / 1e7, 0)  # in Cr
                    quarterly_rev.append(rev_val)

                    np_val = 0
                    if "Net Income" in q_fin.index:
                        np_val = safe_round(q_fin.loc["Net Income", col] / 1e7, 0)
                    elif q_inc is not None and "Net Income" in q_inc.index:
                        np_val = safe_round(q_inc.loc["Net Income", col] / 1e7, 0)
                    quarterly_profit.append(np_val)

        except Exception as e:
            print(f"    Quarterly data error for {key}: {e}")

        # If no quarterly data, use placeholder
        if not quarterly_rev:
            quarterly_rev = [0, 0, 0, 0, 0]
            quarterly_profit = [0, 0, 0, 0, 0]
            quarter_labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]

        # EPS growth (YoY % approximation from quarterly profits)
        for i in range(len(quarterly_profit)):
            if i > 0 and quarterly_profit[i - 1] != 0:
                growth = ((quarterly_profit[i] - quarterly_profit[i - 1]) / abs(quarterly_profit[i - 1])) * 100
                quarterly_eps_growth.append(safe_round(growth, 1))
            else:
                quarterly_eps_growth.append(0)

        # Margins (use current from info, repeat for chart)
        quarterly_gm = [gross_margin] * len(quarter_labels)
        quarterly_om = [operating_margin] * len(quarter_labels)
        quarterly_nm = [net_margin] * len(quarter_labels)

        # --- TECHNICAL INDICATORS ---
        sma20 = safe_round(compute_sma(close, 20).iloc[-1])
        sma50 = safe_round(compute_sma(close, 50).iloc[-1])
        sma200_series = compute_sma(close, 200)
        sma200 = safe_round(sma200_series.iloc[-1]) if len(close) >= 200 else safe_round(compute_sma(close, min(len(close), 200)).iloc[-1])

        rsi_series = compute_rsi(close)
        rsi_current = safe_round(rsi_series.iloc[-1])

        macd_line, signal_line, macd_hist = compute_macd(close)
        macd_current = safe_round(macd_line.iloc[-1])
        macd_signal = safe_round(signal_line.iloc[-1])

        # --- WEEKLY PRICE HISTORY (last 14 weeks for charts) ---
        weekly = close.resample("W").last().dropna()
        price_history = [safe_round(p) for p in weekly.tail(14).tolist()]
        if len(price_history) < 14:
            # Pad with daily data if not enough weekly
            price_history = [safe_round(p) for p in close.tail(14).tolist()]

        # RSI history (last 14 data points, sampled)
        rsi_full = compute_rsi(close).dropna()
        step = max(1, len(rsi_full) // 14)
        rsi_history = [safe_round(rsi_full.iloc[i]) for i in range(0, len(rsi_full), step)][-14:]
        if len(rsi_history) < 14:
            rsi_history = [rsi_current] * 14

        # MACD history (last 14 points)
        macd_full = macd_line.dropna()
        sig_full = signal_line.dropna()
        step_m = max(1, len(macd_full) // 14)
        macd_history = [safe_round(macd_full.iloc[i]) for i in range(0, len(macd_full), step_m)][-14:]
        macd_sig_history = [safe_round(sig_full.iloc[i]) for i in range(0, len(sig_full), step_m)][-14:]
        if len(macd_history) < 14:
            macd_history = [macd_current] * 14
            macd_sig_history = [macd_signal] * 14

        # --- HOLDINGS ---
        promoter_hold = 0
        fii_hold = 0
        dii_hold = 0
        try:
            holders = stock.major_holders
            if holders is not None and not holders.empty:
                # yfinance major_holders gives percentages
                for _, row in holders.iterrows():
                    val_str = str(row.iloc[0]).replace("%", "").strip()
                    label = str(row.iloc[1]).lower()
                    try:
                        val_num = float(val_str)
                    except:
                        val_num = 0
                    if "insider" in label:
                        promoter_hold = safe_round(val_num)
                    elif "institution" in label:
                        fii_hold = safe_round(val_num)
        except:
            pass

        # --- SENTIMENT SCORE ---
        # Technical sentiment (based on indicators)
        tech_signals = 0
        tech_total = 5
        if rsi_current > 50: tech_signals += 1
        if macd_current > macd_signal: tech_signals += 1
        if current_price > sma20: tech_signals += 1
        if current_price > sma50: tech_signals += 1
        if current_price > sma200: tech_signals += 1
        tech_sentiment = int((tech_signals / tech_total) * 100)

        # Fundamental sentiment
        fund_signals = 0
        fund_total = 5
        if pe > 0 and pe < 30: fund_signals += 1
        if roe > 12: fund_signals += 1
        if debt_equity < 1.5: fund_signals += 1
        if net_margin > 10: fund_signals += 1
        if div_yield > 0.5: fund_signals += 1
        fund_sentiment = int((fund_signals / fund_total) * 100)

        overall_sentiment = int((tech_sentiment + fund_sentiment) / 2)

        # --- BUILD OUTPUT ---
        stock_data = {
            "name": meta["name"],
            "sector": meta["sector"],
            "price": current_price,
            "change": change_pct,
            "cAbs": change_abs,
            "mCap": format_market_cap(market_cap),
            "pe": pe,
            "eps": eps,
            "epsG": quarterly_eps_growth if quarterly_eps_growth else [0, 0, 0, 0, 0],
            "bv": book_value,
            "dy": div_yield,
            "de": debt_equity,
            "roe": roe,
            "rev": quarterly_rev,
            "np": quarterly_profit,
            "gm": quarterly_gm,
            "om": quarterly_om,
            "nm": quarterly_nm,
            "qLabels": quarter_labels,
            "h52": high_52,
            "l52": low_52,
            "s20": sma20,
            "s50": sma50,
            "s200": sma200,
            "rsi": rsi_current,
            "macd": macd_current,
            "ms": macd_signal,
            "pH": price_history,
            "rH": rsi_history,
            "mH": macd_history,
            "mSH": macd_sig_history,
            "ph": promoter_hold,
            "fii": fii_hold,
            "dii": dii_hold,
            "st": {
                "t": tech_sentiment,
                "f": fund_sentiment,
                "a": overall_sentiment
            }
        }

        print(f"    ✓ {key}: ₹{current_price} ({change_pct:+}%)")
        return stock_data

    except Exception as e:
        print(f"    ✗ ERROR fetching {key}: {e}")
        return None


# ============================================
# MAIN — FETCH ALL STOCKS
# ============================================
def main():
    print("=" * 60)
    print("TradersFootprint — Nifty 50 Data Update")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 60)

    all_data = {}
    success = 0
    failed = 0

    for key, meta in NIFTY50_TICKERS.items():
        result = fetch_stock_data(key, meta)
        if result:
            all_data[key] = result
            success += 1
        else:
            failed += 1

    # --- SAVE JSON ---
    os.makedirs("data", exist_ok=True)
    output = {
        "lastUpdated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "stockCount": len(all_data),
        "stocks": all_data
    }

    output_path = "data/nifty50_data.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)

    print("\n" + "=" * 60)
    print(f"✓ Done! {success} stocks fetched, {failed} failed.")
    print(f"✓ Output: {output_path}")
    print(f"✓ File size: {os.path.getsize(output_path) / 1024:.1f} KB")
    print("=" * 60)


if __name__ == "__main__":
    main()
