from SmartApi import SmartConnect
import pyotp
import datetime
import pandas as pd
import numpy as np
import time
import logging
from flask import Flask, render_template_string, jsonify, request
import threading
import pytz
import csv
import os

# Setup Logging
logging.basicConfig(
    filename="nifty200_pivot_scan.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Credentials
API_KEY = "G7GJN3yQ"
CLIENT_ID = "P57150421"
MPIN = "5214"
TOTP_SECRET = "IOKPXZMIR65Y2AD7BT4BVIQIAM"

# Initialize SmartAPI
smartApi = SmartConnect(api_key=API_KEY)
totp = pyotp.TOTP(TOTP_SECRET).now()
data = smartApi.generateSession(CLIENT_ID, MPIN, totp)
if not data["status"]:
    logging.error(f"Login Failed: {data['message']}")
    print(f"❌ Login Failed: {data['message']}")
    exit()
access_token = data["data"]["jwtToken"].replace("Bearer ", "")
smartApi.setAccessToken(access_token)
logging.info(f"Login Successful, Access Token: {access_token[:20]}...")
print(f"✅ Login Successful, Access Token: {access_token[:20]}...")

# Flask App
app = Flask(__name__)

# Load Stocks from CSV with Sector
def load_stocks_from_csv(csv_path="stocks.csv"):
    if not os.path.exists(csv_path):
        logging.error(f"CSV file {csv_path} not found")
        print(f"❌ CSV file {csv_path} not found")
        return {}
    stocks = {}
    sectors = set()
    try:
        with open(csv_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                symbol = row["symbol"].strip().upper()
                token = row["token"].strip()
                sector = row["sector"].strip()
                if token and token.isdigit():
                    stocks[symbol] = {"token": token, "sector": sector}
                    sectors.add(sector)
                else:
                    logging.warning(f"Invalid token for {symbol}: '{token}' - Skipping")
                    print(f"⚠️ Invalid token for {symbol}: '{token}' - Skipping")
        logging.info(f"Loaded {len(stocks)} stocks from {csv_path} with {len(sectors)} sectors")
        print(f"✅ Loaded {len(stocks)} stocks from {csv_path} with {len(sectors)} sectors")
        return stocks, sorted(sectors)
    except Exception as e:
        logging.error(f"Error reading CSV {csv_path}: {e}")
        print(f"❌ Error reading CSV {csv_path}: {e}")
        return {}, []

# Load stocks and sectors
nifty_200_stocks, available_sectors = load_stocks_from_csv()

# Global storage
live_data_store = {}
active_breakouts = {}
prev_candle_store = {}  # New: Store previous candle data
scan_mode = "live"
historical_date = None
scanner_thread = None

# Fetch Previous Day's Data
def fetch_prev_day_data(token, target_date=None):
    ist = pytz.timezone("Asia/Kolkata")
    if target_date:
        try:
            now = datetime.datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=ist)
        except ValueError:
            logging.error(f"Invalid target_date format: {target_date}")
            return None
    else:
        now = datetime.datetime.now(ist)
    
    prev_day = now - datetime.timedelta(days=1)
    two_days_ago = now - datetime.timedelta(days=2)
    
    if prev_day > datetime.datetime.now(ist):
        logging.warning(f"Cannot fetch previous day data for future date: {prev_day.date()}")
        return None
    
    params = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_DAY",
        "fromdate": two_days_ago.strftime("%Y-%m-%d 09:15"),
        "todate": prev_day.strftime("%Y-%m-%d 15:30"),
    }
    for attempt in range(5):
        try:
            response = smartApi.getCandleData(params)
            if response.get("status") and response["data"]:
                df = pd.DataFrame(response["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
                return df.iloc[-1]
            logging.warning(f"Token {token} - No prev day data: {response.get('message', 'No data')}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Token {token} - Prev Day Error (Attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
    return None

# Fetch 5-Minute Opening Range (9:15–9:20 AM IST)
def fetch_opening_range(token, target_date=None):
    ist = pytz.timezone("Asia/Kolkata")
    if target_date:
        try:
            today = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
        except ValueError:
            logging.error(f"Invalid target_date format: {target_date}")
            return None
    else:
        today = datetime.datetime.now(ist).date()
    
    start_time = datetime.datetime(today.year, today.month, today.day, 9, 15, tzinfo=ist)
    end_time = datetime.datetime(today.year, today.month, today.day, 9, 20, tzinfo=ist)
    
    if start_time > datetime.datetime.now(ist):
        logging.warning(f"Cannot fetch opening range for future date: {today}")
        return None
    
    params = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "FIVE_MINUTE",
        "fromdate": start_time.strftime("%Y-%m-%d %H:%M"),
        "todate": end_time.strftime("%Y-%m-%d %H:%M"),
    }
    for attempt in range(5):
        try:
            response = smartApi.getCandleData(params)
            if response.get("status") and response["data"]:
                df = pd.DataFrame(response["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
                return {"high": df["high"].iloc[0], "low": df["low"].iloc[0]}
            logging.warning(f"Token {token} - No opening range data: {response.get('message', 'No data')}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Token {token} - Opening Range Error (Attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
    return None

# Calculate Pivot Points
def calculate_pivots(prev_day):
    prevHigh, prevLow, prevClose = prev_day["high"], prev_day["low"], prev_day["close"]
    P = (prevHigh + prevLow + prevClose) / 3
    R1 = P * 2 - prevLow
    S1 = P * 2 - prevHigh
    R2 = P + (prevHigh - prevLow)
    S2 = P - (prevHigh - prevLow)
    R3 = P * 2 + (prevHigh - 2 * prevLow)
    S3 = P * 2 - (2 * prevHigh - prevLow)
    R4 = P * 3 + (prevHigh - 3 * prevLow)
    S4 = P * 3 - (3 * prevHigh - prevLow)
    R5 = P * 4 + (prevHigh - 4 * prevLow)
    S5 = P * 4 - (4 * prevHigh - prevLow)
    return {
        "P": P,
        "R1": R1,
        "R2": R2,
        "R3": R3,
        "R4": R4,
        "R5": R5,
        "S1": S1,
        "S2": S2,
        "S3": S3,
        "S4": S4,
        "S5": S5
    }

# Fetch Latest 5-Minute Candle
def fetch_latest_candle(symbol, token, target_date=None):
    ist = pytz.timezone("Asia/Kolkata")
    if target_date:
        try:
            now = datetime.datetime.strptime(f"{target_date} 15:30", "%Y-%m-%d %H:%M").replace(tzinfo=ist)
            start_dt = datetime.datetime.strptime(f"{target_date} 09:15", "%Y-%m-%d %H:%M").replace(tzinfo=ist)
        except ValueError:
            logging.error(f"Invalid target_date format: {target_date}")
            return None
    else:
        now = datetime.datetime.now(ist)
        start_dt = now - datetime.timedelta(minutes=10)  # Fetch last 10 minutes to get previous candle
    
    if now > datetime.datetime.now(ist):
        logging.warning(f"Cannot fetch candle for future date: {now.date()}")
        return None
    
    params = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "FIVE_MINUTE",
        "fromdate": start_dt.strftime("%Y-%m-%d %H:%M"),
        "todate": now.strftime("%Y-%m-%d %H:%M"),
    }
    for attempt in range(5):
        try:
            response = smartApi.getCandleData(params)
            if response.get("status") and response["data"]:
                df = pd.DataFrame(response["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
                if len(df) >= 2:  # Ensure at least 2 candles
                    return df.iloc[-2:]  # Return last 2 candles
                elif len(df) == 1:
                    return df.iloc[[-1]]  # Return latest if only 1 candle
            logging.warning(f"Token {token} ({symbol}) - No candle data: {response.get('message', 'No data')}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Token {token} ({symbol}) - Candle Error (Attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
    return None

# Fetch Historical 5-Minute Candles for a Day
def fetch_historical_candles(token, target_date):
    ist = pytz.timezone("Asia/Kolkata")
    try:
        start_time = datetime.datetime.strptime(f"{target_date} 09:15", "%Y-%m-%d %H:%M").replace(tzinfo=ist)
        end_time = datetime.datetime.strptime(f"{target_date} 15:30", "%Y-%m-%d %H:%M").replace(tzinfo=ist)
    except ValueError:
        logging.error(f"Invalid target_date format: {target_date}")
        return None
    
    if end_time > datetime.datetime.now(ist):
        logging.warning(f"Cannot fetch historical candles for future date: {target_date}")
        return None
    
    params = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "FIVE_MINUTE",
        "fromdate": start_time.strftime("%Y-%m-%d %H:%M"),
        "todate": end_time.strftime("%Y-%m-%d %H:%M"),
    }
    for attempt in range(5):
        try:
            response = smartApi.getCandleData(params)
            if response.get("status") and response["data"]:
                df = pd.DataFrame(response["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
                return df
            logging.warning(f"Token {token} - No historical data: {response.get('message', 'No data')}")
            time.sleep(2 ** attempt)
        except Exception as e:
            logging.error(f"Token {token} - Historical Data Error (Attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
    return None

# Initialize Pivot Points and Opening Range
def initialize_pivot_points_and_range(target_date=None):
    pivot_points = {}
    opening_ranges = {}
    logging.info(f"Starting pivot and range calculation for {len(nifty_200_stocks)} stocks")
    for symbol, data in nifty_200_stocks.items():
        token = data["token"]
        if not token or not token.isdigit():
            logging.warning(f"{symbol}: Invalid token '{token}' - Skipping")
            continue
        logging.info(f"Processing {symbol} (token: {token})")
        prev_day = fetch_prev_day_data(token, target_date)
        if prev_day is None:
            logging.warning(f"{symbol}: Failed to fetch previous day data")
            continue
        opening_range = fetch_opening_range(token, target_date)
        if opening_range is None:
            logging.warning(f"{symbol}: Failed to fetch opening range")
            continue
        pivot_points[symbol] = calculate_pivots(prev_day)
        opening_ranges[symbol] = opening_range
        logging.info(f"{symbol} Pivot Levels: {pivot_points[symbol]}, Opening Range: {opening_range}")
        print(f"✅ {symbol} Pivot Levels and Opening Range Calculated")
        time.sleep(0.2)  # Avoid rate limits
    logging.info(f"Completed pivot and range calculation. Processed {len(pivot_points)} stocks")
    return pivot_points, opening_ranges

# New Function to Save live_data_store to CSV
def save_to_csv():
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.datetime.now(ist).strftime("%Y-%m-%d")
    csv_file = f"scan_{today}.csv"
    try:
        data_list = [
            {
                "symbol": symbol,
                "sector": data["sector"],
                "p": data["p"],
                "r1": data["r1"],
                "s1": data["s1"],
                "breaking_level": data["breaking_level"],
                "breaking_type": data["breaking_type"],
                "breakout_timestamp": data["breakout_timestamp"],
                "status": data["status"]
            }
            for symbol, data in live_data_store.items()
        ]
        if data_list:
            df = pd.DataFrame(data_list)
            df.to_csv(csv_file, index=False)
            logging.info(f"Saved live scan data to {csv_file} with {len(data_list)} records")
            print(f"💾 Saved live scan data to {csv_file}")
        else:
            logging.warning(f"No data to save to {csv_file}")
            print(f"⚠️ No data to save to {csv_file}")
    except Exception as e:
        logging.error(f"Error saving to {csv_file}: {e}")
        print(f"❌ Error saving to {csv_file}: {e}")

# Updated Live Market Scanner with Two-Candle Confirmation
def live_market_scan():
    global live_data_store, active_breakouts, prev_candle_store
    print("Starting live market scan...")
    pivot_points, opening_ranges = initialize_pivot_points_and_range()
    last_candle_time = {symbol: None for symbol in nifty_200_stocks}
    ist = pytz.timezone("Asia/Kolkata")

    while scan_mode == "live" and threading.current_thread().is_alive():
        scan_time = datetime.datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Scanning at {scan_time}")
        for symbol, data in nifty_200_stocks.items():
            token = data["token"]
            if not token or not token.isdigit():
                continue
            if symbol not in pivot_points or symbol not in opening_ranges:
                continue
            candles = fetch_latest_candle(symbol, token)  # Fetch last 2 candles
            if candles is None or len(candles) < 1:
                continue

            current_candle = candles.iloc[-1]
            prev_candle = candles.iloc[-2] if len(candles) >= 2 else None
            timestamp = current_candle["timestamp"]
            if last_candle_time[symbol] == timestamp:
                continue
            last_candle_time[symbol] = timestamp

            close, high, low = current_candle["close"], current_candle["high"], current_candle["low"]
            prev_close = prev_candle["close"] if prev_candle is not None else None
            prev_high = prev_candle["high"] if prev_candle is not None else None
            prev_low = prev_candle["low"] if prev_candle is not None else None
            prev_timestamp = prev_candle["timestamp"] if prev_candle is not None else None

            pivots = pivot_points[symbol]
            opening_range = opening_ranges[symbol]
            levels = {
                "R5": pivots["R5"], "R4": pivots["R4"], "R3": pivots["R3"], "R2": pivots["R2"], "R1": pivots["R1"],
                "S1": pivots["S1"], "S2": pivots["S2"], "S3": pivots["S3"], "S4": pivots["S4"], "S5": pivots["S5"]
            }

            # Check for highest breakout level in current candle
            breakout_level = None
            breakout_type = None
            breakout_value = None
            if high > opening_range["high"]:
                for level_name in ["R5", "R4", "R3", "R2", "R1"]:
                    level_value = levels[level_name]
                    if high > level_value:
                        breakout_level = level_name
                        breakout_type = "Long"
                        breakout_value = level_value
                        break
            elif low < opening_range["low"]:
                for level_name in ["S5", "S4", "S3", "S2", "S1"]:
                    level_value = levels[level_name]
                    if low < level_value:
                        breakout_level = level_name
                        breakout_type = "Short"
                        breakout_value = level_value
                        break

            # Two-candle confirmation logic
            if breakout_level and prev_candle is not None:
                if breakout_type == "Long" and prev_close is not None and prev_close > breakout_value:
                    if close > breakout_value:  # Confirm with current close
                        if symbol not in active_breakouts or active_breakouts[symbol]["level"] != breakout_level:
                            active_breakouts[symbol] = {
                                "level": breakout_level,
                                "type": breakout_type,
                                "value": breakout_value,
                                "timestamp": timestamp  # Set to confirmation candle time
                            }
                            logging.info(f"{symbol} - Confirmed {breakout_type} breakout at {breakout_level} (prev_close: {prev_close}, close: {close}, level: {breakout_value}) at {timestamp}")
                            print(f"✅ {symbol} - Confirmed {breakout_type} breakout at {breakout_level} at {timestamp}")
                    else:
                        if symbol in active_breakouts:
                            del active_breakouts[symbol]  # Invalidate if close falls below
                            logging.info(f"{symbol} - Long breakout at {breakout_level} invalidated at {timestamp}")
                            print(f"❌ {symbol} - Long breakout at {breakout_level} invalidated at {timestamp}")
                elif breakout_type == "Short" and prev_close is not None and prev_close < breakout_value:
                    if close < breakout_value:  # Confirm with current close
                        if symbol not in active_breakouts or active_breakouts[symbol]["level"] != breakout_level:
                            active_breakouts[symbol] = {
                                "level": breakout_level,
                                "type": breakout_type,
                                "value": breakout_value,
                                "timestamp": timestamp  # Set to confirmation candle time
                            }
                            logging.info(f"{symbol} - Confirmed {breakout_type} breakout at {breakout_level} (prev_close: {prev_close}, close: {close}, level: {breakout_value}) at {timestamp}")
                            print(f"✅ {symbol} - Confirmed {breakout_type} breakout at {breakout_level} at {timestamp}")
                    else:
                        if symbol in active_breakouts:
                            del active_breakouts[symbol]  # Invalidate if close rises above
                            logging.info(f"{symbol} - Short breakout at {breakout_level} invalidated at {timestamp}")
                            print(f"❌ {symbol} - Short breakout at {breakout_level} invalidated at {timestamp}")

            # Store current candle as previous for next iteration
            if prev_candle is not None:
                prev_candle_store[symbol] = {
                    "close": prev_close,
                    "high": prev_high,
                    "low": prev_low,
                    "timestamp": prev_timestamp
                }

            live_data_store[symbol] = {
                
                "r1": pivots["R1"],
               
                "s1": pivots["S1"],
               
                "p": pivots["P"],
               
                "breaking_level": active_breakouts.get(symbol, {}).get("level", "-"),
                "breaking_type": active_breakouts.get(symbol, {}).get("type", "-"),
                
                "breakout_timestamp": active_breakouts.get(symbol, {}).get("timestamp", "-"),
                "status": "Confirmed" if symbol in active_breakouts else "-",
                "sector": nifty_200_stocks[symbol]["sector"]
            }
            time.sleep(0.1)  # Avoid rate limits

        # Save to CSV after each scan cycle
        save_to_csv()
        time.sleep(60)

# Historical Market Scanner (unchanged)
def historical_market_scan(target_date):
    global live_data_store, active_breakouts
    print(f"Starting historical market scan for {target_date}...")
    live_data_store.clear()
    active_breakouts.clear()
    pivot_points, opening_ranges = initialize_pivot_points_and_range(target_date)
    ist = pytz.timezone("Asia/Kolkata")

    for symbol, data in nifty_200_stocks.items():
        token = data["token"]
        if not token or not token.isdigit():
            continue
        if symbol not in pivot_points or symbol not in opening_ranges:
            continue
        candles = fetch_historical_candles(token, target_date)
        if candles is None or candles.empty:
            logging.warning(f"{symbol}: No historical candles found for {target_date}")
            continue

        pivots = pivot_points[symbol]
        opening_range = opening_ranges[symbol]
        levels = {
            "R5": pivots["R5"], "R4": pivots["R4"], "R3": pivots["R3"], "R2": pivots["R2"], "R1": pivots["R1"],
            "S1": pivots["S1"], "S2": pivots["S2"], "S3": pivots["S3"], "S4": pivots["S4"], "S5": pivots["S5"]
        }

        for idx in range(len(candles) - 1):
            current_candle = candles.iloc[idx + 1]
            prev_candle = candles.iloc[idx]
            timestamp = current_candle["timestamp"]
            close, high, low = current_candle["close"], current_candle["high"], current_candle["low"]
            prev_close = prev_candle["close"]
            prev_high = prev_candle["high"]
            prev_low = prev_candle["low"]

            # Check for highest breakout level in current candle
            breakout_level = None
            breakout_type = None
            breakout_value = None
            if high > opening_range["high"]:
                for level_name in ["R5", "R4", "R3", "R2", "R1"]:
                    level_value = levels[level_name]
                    if high > level_value:
                        breakout_level = level_name
                        breakout_type = "Long"
                        breakout_value = level_value
                        break
            elif low < opening_range["low"]:
                for level_name in ["S5", "S4", "S3", "S2", "S1"]:
                    level_value = levels[level_name]
                    if low < level_value:
                        breakout_level = level_name
                        breakout_type = "Short"
                        breakout_value = level_value
                        break

            # Two-candle confirmation logic
            if breakout_level and prev_close is not None:
                if breakout_type == "Long" and prev_close > breakout_value:
                    if close > breakout_value:  # Confirm with current close
                        if symbol not in active_breakouts or active_breakouts[symbol]["level"] != breakout_level:
                            active_breakouts[symbol] = {
                                "level": breakout_level,
                                "type": breakout_type,
                                "value": breakout_value,
                                "timestamp": timestamp
                            }
                            logging.info(f"{symbol} - Confirmed {breakout_type} breakout at {breakout_level} (prev_close: {prev_close}, close: {close}, level: {breakout_value}) at {timestamp}")
                            print(f"✅ {symbol} - Confirmed {breakout_type} breakout at {breakout_level} at {timestamp}")
                    else:
                        if symbol in active_breakouts:
                            del active_breakouts[symbol]
                            logging.info(f"{symbol} - Long breakout at {breakout_level} invalidated at {timestamp}")
                            print(f"❌ {symbol} - Long breakout at {breakout_level} invalidated at {timestamp}")
                elif breakout_type == "Short" and prev_close < breakout_value:
                    if close < breakout_value:  # Confirm with current close
                        if symbol not in active_breakouts or active_breakouts[symbol]["level"] != breakout_level:
                            active_breakouts[symbol] = {
                                "level": breakout_level,
                                "type": breakout_type,
                                "value": breakout_value,
                                "timestamp": timestamp
                            }
                            logging.info(f"{symbol} - Confirmed {breakout_type} breakout at {breakout_level} (prev_close: {prev_close}, close: {close}, level: {breakout_value}) at {timestamp}")
                            print(f"✅ {symbol} - Confirmed {breakout_type} breakout at {breakout_level} at {timestamp}")
                    else:
                        if symbol in active_breakouts:
                            del active_breakouts[symbol]
                            logging.info(f"{symbol} - Short breakout at {breakout_level} invalidated at {timestamp}")
                            print(f"❌ {symbol} - Short breakout at {breakout_level} invalidated at {timestamp}")

            live_data_store[symbol] = {
                
                "r1": pivots["R1"],
               
                "s1": pivots["S1"],
               
                "p": pivots["P"],
                
                "breaking_level": active_breakouts.get(symbol, {}).get("level", "-"),
                "breaking_type": active_breakouts.get(symbol, {}).get("type", "-"),
                
                "breakout_timestamp": active_breakouts.get(symbol, {}).get("timestamp", "-"),
                "status": "Confirmed" if symbol in active_breakouts else "-",
                "sector": nifty_200_stocks[symbol]["sector"]
            }
        time.sleep(0.2)  # Avoid rate limits

    logging.info(f"Historical scan for {target_date} completed with {len(live_data_store)} stocks.")
    print(f"✅ Historical scan for {target_date} completed with {len(live_data_store)} stocks.")

# Switch Scan Mode
@app.route('/set-mode', methods=['POST'])
def set_mode():
    global scan_mode, historical_date, scanner_thread, prev_candle_store
    mode = request.form.get('mode')
    date = request.form.get('date')
    
    live_data_store.clear()
    active_breakouts.clear()
    prev_candle_store.clear()
    
    if mode == "live":
        scan_mode = "live"
        historical_date = None
        logging.info("Switched to Live Market Scan")
        print("✅ Switched to Live Market Scan")
        if scanner_thread is None or not scanner_thread.is_alive():
            scanner_thread = threading.Thread(target=live_market_scan, name="live_scan", daemon=True)
            scanner_thread.start()
    elif mode == "historical" and date:
        try:
            target_date = datetime.datetime.strptime(date, "%Y-%m-%d")
            if target_date.date() > datetime.datetime.now().date():
                return jsonify({"status": "error", "message": "Cannot scan future dates"})
            scan_mode = "historical"
            historical_date = date
            logging.info(f"Switched to Historical Scan for {date}")
            print(f"✅ Switched to Historical Scan for {date}")
            historical_market_scan(date)
        except ValueError:
            return jsonify({"status": "error", "message": "Invalid date format (use YYYY-MM-DD)"})
    else:
        return jsonify({"status": "error", "message": "Invalid mode or missing date"})
    
    return jsonify({"status": "success", "mode": scan_mode, "date": historical_date})

# Live Data Endpoint with Sorting
@app.route('/live-data', methods=['GET'])
def get_live_data():
    data_list = [(symbol, data) for symbol, data in live_data_store.items()]
    sorted_data = sorted(
        data_list,
        key=lambda x: (
            x[1]["status"] != "Confirmed",
            x[1]["breakout_timestamp"] if x[1]["status"] == "Confirmed" else "9999-12-31 23:59:59",
            x[0]
        ),
        reverse=True
    )
    sorted_dict = {symbol: data for symbol, data in sorted_data}
    return jsonify({"mode": scan_mode, "date": historical_date, "data": sorted_dict, "sectors": available_sectors})

# User-Friendly Live Market Page (unchanged)

@app.route('/live-market')
def live_market():
    return render_template_string("""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Nifty 200 Pivot Breakouts</title>
    <style>
        :root {
            --primary-color: #4f46e5; /* Indigo for primary elements */
            --success-color: #10b981; /* Emerald for confirmed/long */
            --danger-color: #ef4444; /* Red for short */
            --background-color: #e5e7eb; /* Light gray background */
            --card-background: rgba(255, 255, 255, 0.95); /* Glassmorphism */
            --card-backdrop: blur(12px);
            --text-color: #111827; /* Deep gray for text */
            --border-color: rgba(209, 213, 219, 0.5); /* Semi-transparent */
            --accent-color: #8b5cf6; /* Violet for highlights */
            --table-header-bg: linear-gradient(135deg, #4f46e5, #7c3aed); /* Gradient */
            --link-color: #3b82f6; /* Blue for symbol links */
            --shadow: 0 4px 6px rgba(0, 0, 0, 0.1), 0 1px 3px rgba(0, 0, 0, 0.08);
        }

        * {
            box-sizing: border-box;
            margin: 0;
            padding: 0;
        }

        body {
            font-family: 'Poppins', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: linear-gradient(135deg, #e5e7eb, #d1d5db);
            color: var(--text-color);
            line-height: 1.6;
            padding: 2rem;
            overflow-x: hidden;
        }

        .container {
            max-width: 1600px;
            margin: 0 auto;
        }

        h1 {
            text-align: center;
            color: var(--primary-color);
            font-size: 2.5rem;
            margin-bottom: 2rem;
            font-weight: 700;
            animation: slideIn 0.5s ease-out;
        }

        .status-bar {
            background: var(--card-background);
            backdrop-filter: var(--card-backdrop);
            padding: 1rem;
            border-radius: 1rem;
            text-align: center;
            font-weight: 500;
            margin-bottom: 2rem;
            font-size: 1rem;
            color: var(--text-color);
            box-shadow: var(--shadow);
            animation: slideIn 0.6s ease-out;
        }

        .controls {
            display: flex;
            flex-wrap: wrap;
            gap: 1rem;
            margin-bottom: 2rem;
            align-items: center;
            background: var(--card-background);
            backdrop-filter: var(--card-backdrop);
            padding: 1.5rem;
            border-radius: 1rem;
            box-shadow: var(--shadow);
            animation: slideIn 0.7s ease-out;
        }

        .controls input, .controls select, .controls button {
            padding: 0.75rem 1rem;
            font-size: 0.95rem;
            border: 1px solid var(--border-color);
            border-radius: 0.5rem;
            transition: all 0.3s ease;
            background: rgba(255, 255, 255, 0.9);
        }

        .controls input:focus, .controls select:focus {
            outline: none;
            border-color: var(--primary-color);
            box-shadow: 0 0 0 4px rgba(79, 70, 229, 0.2);
        }

        .controls button {
            background: var(--primary-color);
            color: white;
            border: none;
            cursor: pointer;
            font-weight: 600;
            padding: 0.75rem 1.5rem;
            border-radius: 0.5rem;
            transition: transform 0.2s, background 0.3s;
        }

        .controls button:hover {
            background: #4338ca;
            transform: translateY(-2px);
        }

        .controls button:active {
            transform: translateY(0);
        }

        .toggle-container {
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }

        .toggle-switch {
            position: relative;
            width: 48px;
            height: 24px;
        }

        .toggle-switch input {
            opacity: 0;
            width: 0;
            height: 0;
        }

        .slider {
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: #d1d5db;
            border-radius: 9999px;
            transition: background 0.3s ease;
        }

        .slider:before {
            position: absolute;
            content: "";
            height: 20px;
            width: 20px;
            left: 2px;
            bottom: 2px;
            background: white;
            border-radius: 50%;
            transition: transform 0.3s ease;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.2);
        }

        input:checked + .slider {
            background: var(--primary-color);
        }

        input:checked + .slider:before {
            transform: translateX(24px);
        }

        .toggle-label {
            font-size: 0.95rem;
            color: var(--text-color);
            font-weight: 500;
        }

        .table-container {
            background: var(--card-background);
            backdrop-filter: var(--card-backdrop);
            border-radius: 1rem;
            box-shadow: var(--shadow);
            overflow-x: auto;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.95rem;
        }

        th, td {
            padding: 1rem;
            text-align: center;
            border-bottom: 1px solid var(--border-color);
        }

        th {
            background: var(--table-header-bg);
            color: white;
            font-weight: 600;
            position: sticky;
            top: 0;
            z-index: 10;
            cursor: pointer;
            user-select: none;
            transition: background 0.3s ease;
        }

        th:hover {
            background: linear-gradient(135deg, #4338ca, #6d28d9);
        }

        th.sort-asc::after {
            content: ' ▲';
        }

        th.sort-desc::after {
            content: ' ▼';
        }

        tr {
            opacity: 0;
            animation: fadeIn 0.5s ease forwards;
            animation-delay: calc(var(--row-index) * 0.05s);
        }

        tr:nth-child(even) {
            background: rgba(243, 244, 246, 0.5);
        }

        tr:hover {
            background: rgba(229, 231, 235, 0.8);
            transform: translateY(-2px);
            transition: all 0.2s ease;
        }

        .long {
            color: var(--success-color);
            font-weight: 600;
        }

        .short {
            color: var(--danger-color);
            font-weight: 600;
        }

        .confirmed {
            color: var(--success-color);
            font-weight: 600;
        }

        .tooltip {
            position: relative;
            text-decoration: underline;
            text-decoration-style: dotted;
            text-decoration-color: var(--accent-color);
            cursor: help;
        }

        .tooltip:hover::after {
            content: attr(data-tooltip);
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: rgba(17, 24, 39, 0.95);
            color: white;
            padding: 0.5rem 1rem;
            border-radius: 0.5rem;
            font-size: 0.85rem;
            white-space: nowrap;
            z-index: 20;
            animation: fadeIn 0.2s ease;
        }

        .sl-no {
            font-weight: 500;
            color: var(--text-color);
        }

        .symbol-link {
            color: var(--link-color);
            text-decoration: none;
            font-weight: 600;
            transition: color 0.3s ease, transform 0.2s ease;
        }

        .symbol-link:hover {
            color: #1e40af;
            text-decoration: underline;
            transform: translateY(-1px);
        }

        .symbol-link:focus {
            outline: 2px solid var(--primary-color);
            outline-offset: 2px;
            border-radius: 2px;
        }

        @keyframes slideIn {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }

        @keyframes fadeIn {
            from {
                opacity: 0;
            }
            to {
                opacity: 1;
            }
        }

        @keyframes spin {
            to {
                transform: rotate(360deg);
            }
        }

        .loading::after {
            content: '';
            display: inline-block;
            width: 16px;
            height: 16px;
            border: 2px solid var(--primary-color);
            border-top: 2px solid transparent;
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            margin-left: 0.5rem;
        }

        @media (max-width: 768px) {
            body {
                padding: 1rem;
            }

            h1 {
                font-size: 1.8rem;
            }

            .controls {
                flex-direction: column;
                align-items: stretch;
            }

            .controls input, .controls select, .controls button {
                width: 100%;
            }

            table {
                font-size: 0.85rem;
            }

            th, td {
                padding: 0.75rem;
            }
        }

        @media (max-width: 480px) {
            h1 {
                font-size: 1.5rem;
            }

            th, td {
                font-size: 0.8rem;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Nifty 200 Pivot Breakouts</h1>
        <div class="status-bar" id="statusBar">Mode: Live</div>
        <div class="controls">
            <input type="text" id="search" placeholder="Search by symbol..." aria-label="Search stocks by symbol" onkeyup="filterTable()">
            <select id="sectorFilter" aria-label="Filter by sector" onchange="filterTable()">
                <option value="all">All Sectors</option>
                {% for sector in sectors %}
                <option value="{{ sector }}">{{ sector }}</option>
                {% endfor %}
            </select>
            <div class="toggle-container">
                <label class="toggle-switch">
                    <input type="checkbox" id="filterToggle" checked onchange="filterTable()" aria-label="Toggle stock filter">
                    <span class="slider"></span>
                </label>
                <span class="toggle-label" id="filterLabel">All Stocks</span>
            </div>
            <div class="toggle-container">
                <label class="toggle-switch">
                    <input type="checkbox" id="modeToggle" checked onchange="updateScanMode()" aria-label="Toggle scan mode">
                    <span class="slider"></span>
                </label>
                <span class="toggle-label" id="modeLabel">Live Scan</span>
            </div>
            <input type="date" id="historicalDate" style="display: none;" aria-label="Select historical date">
            <button onclick="updateMarketData()" aria-label="Refresh market data">Refresh Now</button>
        </div>
        <div class="table-container">
            <table id="marketTable" role="grid">
                <thead>
                    <tr>
                        <th data-sort="sl_no" class="tooltip" data-tooltip="Serial Number">Sl No</th>
                        <th data-sort="symbol" class="tooltip" data-tooltip="Stock Symbol (Click to view on TradingView)">Symbol</th>
                        <th data-sort="sector" class="tooltip" data-tooltip="Sector">Sector</th>
                        <th data-sort="p" class="tooltip" data-tooltip="Pivot Point (Reference)">P (₹)</th>
                        <th data-sort="r1" class="tooltip" data-tooltip="First Resistance">R1 (₹)</th>
                        <th data-sort="s1" class="tooltip" data-tooltip="First Support">S1 (₹)</th>
                        <th data-sort="breaking_level" class="tooltip" data-tooltip="Current Breakout Level">Breaking Level</th>
                        <th data-sort="breaking_type" class="tooltip" data-tooltip="Breakout Direction">Breakout Type</th>
                        <th data-sort="breakout_timestamp" class="tooltip" data-tooltip="Time of Breakout Confirmation">Breakout Time</th>
                        <th data-sort="status" class="tooltip" data-tooltip="Breakout Status">Status</th>
                    </tr>
                </thead>
                <tbody id="marketBody">
                    <!-- Data will be populated by JavaScript -->
                </tbody>
            </table>
        </div>
    </div>

    <script>
        let sectors = {{ sectors | tojson }};
        let sortConfig = { key: 'status', direction: 'desc' };

        function formatTimestamp(timestamp) {
            if (timestamp === '-') return '-';
            const date = new Date(timestamp);
            return date.toLocaleString('en-US', {
                hour: 'numeric',
                minute: 'numeric',
                hour12: true,
                day: 'numeric',
                month: 'short',
                year: 'numeric'
            });
        }

        function updateMarketData() {
            const statusBar = document.getElementById('statusBar');
            statusBar.textContent = 'Loading...';
            statusBar.classList.add('loading');
            fetch('/live-data')
                .then(response => response.json())
                .then(response => {
                    const data = response.data;
                    const mode = response.mode;
                    const date = response.date;
                    sectors = response.sectors;
                    updateSectorDropdown();
                    statusBar.classList.remove('loading');
                    statusBar.textContent = mode === 'live' ? 'Mode: Live' : `Mode: Historical (Date: ${date})`;
                    const tbody = document.getElementById('marketBody');
                    tbody.innerHTML = '';

                    // Sort data: Confirmed first (by breakout_timestamp desc), then non-confirmed (by symbol)
                    const sortedEntries = Object.entries(data).sort((a, b) => {
                        const aStatus = a[1].status === 'Confirmed';
                        const bStatus = b[1].status === 'Confirmed';
                        if (aStatus && bStatus) {
                            return b[1].breakout_timestamp.localeCompare(a[1].breakout_timestamp); // Newest first
                        } else if (aStatus && !bStatus) {
                            return -1; // Confirmed before non-confirmed
                        } else if (!aStatus && bStatus) {
                            return 1;
                        } else {
                            return a[0].localeCompare(b[0]); // Sort by symbol for non-confirmed
                        }
                    });

                    sortedEntries.forEach(([symbol, info], index) => {
                        const tradingViewUrl = `https://www.tradingview.com/chart/?symbol=NSE:${symbol}`;
                        const row = document.createElement('tr');
                        row.style.setProperty('--row-index', index);
                        row.innerHTML = `
                            <td class="sl-no">${index + 1}</td>
                            <td><a href="${tradingViewUrl}" target="_blank" class="symbol-link" aria-label="View ${symbol} chart on TradingView">${symbol}</a></td>
                            <td>${info.sector}</td>
                            <td>${info.p.toFixed(2)}</td>
                            <td>${info.r1.toFixed(2)}</td>
                            <td>${info.s1.toFixed(2)}</td>
                            <td>${info.breaking_level}</td>
                            <td class="${info.breaking_type === 'Long' ? 'long' : info.breaking_type === 'Short' ? 'short' : ''}">${info.breaking_type}</td>
                            <td>${formatTimestamp(info.breakout_timestamp)}</td>
                            <td class="${info.status === 'Confirmed' ? 'confirmed' : ''}">${info.status === 'Confirmed' ? '✔ Confirmed' : '-'}</td>
                        `;
                        row.dataset.symbol = symbol.toLowerCase();
                        row.dataset.status = info.status;
                        row.dataset.sector = info.sector;
                        row.dataset.p = info.p;
                        row.dataset.r1 = info.r1;
                        row.dataset.s1 = info.s1;
                        row.dataset.breaking_level = info.breaking_level;
                        row.dataset.breaking_type = info.breaking_type;
                        row.dataset.breakout_timestamp = info.breakout_timestamp || '9999-12-31';
                        row.dataset.sl_no = index + 1;
                        tbody.appendChild(row);
                    });

                    sortTable();
                    filterTable();
                })
                .catch(error => {
                    console.error('Error fetching data:', error);
                    statusBar.classList.remove('loading');
                    statusBar.textContent = statusBar.textContent.includes('Historical') ? statusBar.textContent : 'Mode: Live';
                });
        }

        function updateSectorDropdown() {
            const sectorFilter = document.getElementById('sectorFilter');
            const currentValue = sectorFilter.value;
            sectorFilter.innerHTML = '<option value="all">All Sectors</option>';
            sectors.forEach(sector => {
                const option = document.createElement('option');
                option.value = sector;
                option.textContent = sector;
                sectorFilter.appendChild(option);
            });
            sectorFilter.value = currentValue && sectors.includes(currentValue) ? currentValue : 'all';
        }

        function filterTable() {
            const search = document.getElementById('search').value.toLowerCase();
            const filterToggle = document.getElementById('filterToggle').checked;
            const sector = document.getElementById('sectorFilter').value;
            const rows = document.querySelectorAll('#marketTable tbody tr');

            rows.forEach(row => {
                const symbol = row.dataset.symbol;
                const status = row.dataset.status;
                const rowSector = row.dataset.sector;
                const matchesSearch = symbol.includes(search);
                const matchesFilter = filterToggle ? true : status === 'Confirmed';
                const matchesSector = sector === 'all' || rowSector === sector;
                row.style.display = matchesSearch && matchesFilter && matchesSector ? '' : 'none';
            });

            document.getElementById('filterLabel').textContent = filterToggle ? 'All Stocks' : 'Confirmed Breakouts';
        }

        function sortTable() {
            const rows = Array.from(document.querySelectorAll('#marketTable tbody tr'));
            const key = sortConfig.key;
            const direction = sortConfig.direction;

            rows.sort((a, b) => {
                let aValue = a.dataset[key];
                let bValue = b.dataset[key];

                if (key === 'sl_no' || key === 'p' || key === 'r1' || key === 's1') {
                    aValue = parseFloat(aValue);
                    bValue = parseFloat(bValue);
                    return direction === 'asc' ? aValue - bValue : bValue - aValue;
                } else if (key === 'breakout_timestamp') {
                    aValue = aValue === '-' ? '9999-12-31' : aValue;
                    bValue = bValue === '-' ? '9999-12-31' : bValue;
                    return direction === 'asc' ? aValue.localeCompare(bValue) : bValue.localeCompare(aValue);
                } else if (key === 'status') {
                    aValue = aValue === 'Confirmed' ? 1 : 0;
                    bValue = bValue === 'Confirmed' ? 1 : 0;
                    return direction === 'asc' ? aValue - bValue : bValue - aValue;
                } else {
                    return direction === 'asc' ? aValue.localeCompare(bValue) : bValue.localeCompare(aValue);
                }
            });

            const tbody = document.getElementById('marketBody');
            tbody.innerHTML = '';
            rows.forEach((row, index) => {
                row.style.setProperty('--row-index', index);
                tbody.appendChild(row);
            });

            document.querySelectorAll('th').forEach(th => {
                th.classList.remove('sort-asc', 'sort-desc');
                if (th.dataset.sort === key) {
                    th.classList.add(direction === 'asc' ? 'sort-asc' : 'sort-desc');
                }
            });
        }

        function updateScanMode() {
            const modeToggle = document.getElementById('modeToggle');
            const historicalDateInput = document.getElementById('historicalDate');
            const mode = modeToggle.checked ? 'live' : 'historical';
            document.getElementById('modeLabel').textContent = modeToggle.checked ? 'Live Scan' : 'Historical Scan';

            if (mode === 'live') {
                historicalDateInput.style.display = 'none';
                historicalDateInput.value = '';
                fetch('/set-mode', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: `mode=live`
                })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        updateMarketData();
                    } else {
                        alert(data.message);
                    }
                });
            } else {
                historicalDateInput.style.display = 'inline-block';
                if (historicalDateInput.value) {
                    fetch('/set-mode', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                        body: `mode=historical&date=${historicalDateInput.value}`
                    })
                    .then(response => response.json())
                    .then(data => {
                        if (data.status === 'success') {
                            updateMarketData();
                        } else {
                            alert(data.message);
                        }
                    })
                    .catch(error => {
                        console.error('Error switching mode:', error);
                        alert('Error switching to historical mode');
                    });
                }
            }
        }

        document.getElementById('historicalDate').addEventListener('change', () => {
            const date = document.getElementById('historicalDate').value;
            if (date) {
                fetch('/set-mode', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                    body: `mode=historical&date=${date}`
                })
                .then(response => response.json())
                .then(data => {
                    if (data.status === 'success') {
                        updateMarketData();
                    } else {
                        alert(data.message);
                    }
                })
                .catch(error => {
                    console.error('Error switching mode:', error);
                    alert('Error switching to historical mode');
                });
            }
        });

        document.querySelectorAll('th[data-sort]').forEach(th => {
            th.addEventListener('click', () => {
                const key = th.dataset.sort;
                if (sortConfig.key === key) {
                    sortConfig.direction = sortConfig.direction === 'asc' ? 'desc' : 'asc';
                } else {
                    sortConfig.key = key;
                    sortConfig.direction = 'asc';
                }
                sortTable();
            });
        });

        updateMarketData();
        setInterval(() => {
            if (document.getElementById('modeToggle').checked) {
                updateMarketData();
            }
        }, 10000);
    </script>
</body>
</html>
    """, sectors=available_sectors)


if __name__ == "__main__":
    if not nifty_200_stocks:
        print("❌ No stocks loaded. Exiting.")
        exit()
    logging.info(f"Starting Nifty 200 pivot scan on {datetime.datetime.now()}")
    print(f"🚀 Starting Nifty 200 pivot scan on {datetime.datetime.now()}")
    scanner_thread = threading.Thread(target=live_market_scan, name="live_scan", daemon=True)
    scanner_thread.start()
    # Use Render's PORT environment variable or default to 5000
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)