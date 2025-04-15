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
    for symbol, data in nifty_200_stocks.items():
        token = data["token"]
        if not token or not token.isdigit():
            logging.warning(f"{symbol}: Invalid token '{token}' - Skipping")
            print(f"⚠️ {symbol}: Invalid token '{token}' - Skipping")
            continue
        prev_day = fetch_prev_day_data(token, target_date)
        opening_range = fetch_opening_range(token, target_date)
        if prev_day is not None and opening_range is not None:
            pivot_points[symbol] = calculate_pivots(prev_day)
            opening_ranges[symbol] = opening_range
            logging.info(f"{symbol} Pivot Levels: {pivot_points[symbol]}, Opening Range: {opening_range}")
            print(f"✅ {symbol} Pivot Levels and Opening Range Calculated")
        else:
            logging.warning(f"{symbol}: Could not calculate pivot points or opening range")
            print(f"⚠️ {symbol}: Could not calculate pivot points or opening range")
        time.sleep(0.2)  # Avoid rate limits
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
                "close": data["close"],
                "high": data["high"],
                "low": data["low"],
                "p": data["p"],
                "r1": data["r1"],
                "r2": data["r2"],
                "r3": data["r3"],
                "r4": data["r4"],
                "r5": data["r5"],
                "s1": data["s1"],
                "s2": data["s2"],
                "s3": data["s3"],
                "s4": data["s4"],
                "s5": data["s5"],
                "opening_high": data["opening_high"],
                "opening_low": data["opening_low"],
                "breaking_level": data["breaking_level"],
                "breaking_type": data["breaking_type"],
                "timestamp": data["timestamp"],
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
                "close": close,
                "high": high,
                "low": low,
                "r1": pivots["R1"],
                "r2": pivots["R2"],
                "r3": pivots["R3"],
                "r4": pivots["R4"],
                "r5": pivots["R5"],
                "s1": pivots["S1"],
                "s2": pivots["S2"],
                "s3": pivots["S3"],
                "s4": pivots["S4"],
                "s5": pivots["S5"],
                "p": pivots["P"],
                "opening_high": opening_range["high"],
                "opening_low": opening_range["low"],
                "breaking_level": active_breakouts.get(symbol, {}).get("level", "-"),
                "breaking_type": active_breakouts.get(symbol, {}).get("type", "-"),
                "timestamp": timestamp,
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
                "close": close,
                "high": high,
                "low": low,
                "r1": pivots["R1"],
                "r2": pivots["R2"],
                "r3": pivots["R3"],
                "r4": pivots["R4"],
                "r5": pivots["R5"],
                "s1": pivots["S1"],
                "s2": pivots["S2"],
                "s3": pivots["S3"],
                "s4": pivots["S4"],
                "s5": pivots["S5"],
                "p": pivots["P"],
                "opening_high": opening_range["high"],
                "opening_low": opening_range["low"],
                "breaking_level": active_breakouts.get(symbol, {}).get("level", "-"),
                "breaking_type": active_breakouts.get(symbol, {}).get("type", "-"),
                "timestamp": timestamp,
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
        body {
            font-family: 'Segoe UI', Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f0f2f5;
            color: #333;
        }
        .container {
            max-width: 1800px;
            margin: 0 auto;
        }
        h1 {
            text-align: center;
            color: #1a73e8;
            margin-bottom: 20px;
        }
        .controls {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            flex-wrap: wrap;
            gap: 10px;
        }
        .controls input, .controls select, .controls button {
            padding: 8px 12px;
            font-size: 14px;
            border: 1px solid #ddd;
            border-radius: 4px;
            outline: none;
        }
        .controls button {
            background-color: #1a73e8;
            color: white;
            cursor: pointer;
            border: none;
        }
        .controls button:hover {
            background-color: #1557b0;
        }
        .toggle-switch {
            position: relative;
            display: inline-block;
            width: 60px;
            height: 34px;
        }
        .toggle-switch input {
            opacity: 0;
            width: 0;
            height: 0;
        }
        .slider {
            position: absolute;
            cursor: pointer;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background-color: #ccc;
            transition: .4s;
            border-radius: 34px;
        }
        .slider:before {
            position: absolute;
            content: "";
            height: 26px;
            width: 26px;
            left: 4px;
            bottom: 4px;
            background-color: white;
            transition: .4s;
            border-radius: 50%;
        }
        input:checked + .slider {
            background-color: #1a73e8;
        }
        input:checked + .slider:before {
            transform: translateX(26px);
        }
        .toggle-label {
            margin-left: 10px;
            font-size: 14px;
            vertical-align: middle;
        }
        table {
            width: 100%;
            border-collapse: collapse;
            background-color: white;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            border-radius: 8px;
            overflow: hidden;
        }
        th, td {
            padding: 12px;
            text-align: center;
            border-bottom: 1px solid #eee;
        }
        th {
            background-color: #1a73e8;
            color: white;
            font-weight: 600;
        }
        tr:nth-child(even) {
            background-color: #fafafa;
        }
        tr:hover {
            background-color: #f5f5f5;
        }
        .long {
            color: #28a745;
            font-weight: bold;
        }
        .short {
            color: #dc3545;
            font-weight: bold;
        }
        .confirmed {
            color: #28a745;
            font-weight: bold;
        }
        .tooltip {
            position: relative;
            cursor: help;
        }
        .tooltip:hover::after {
            content: attr(data-tooltip);
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background-color: #333;
            color: white;
            padding: 5px 10px;
            border-radius: 4px;
            font-size: 12px;
            white-space: nowrap;
            z-index: 10;
        }
        .status-bar {
            margin-bottom: 20px;
            padding: 10px;
            background-color: #e8f0fe;
            border-radius: 4px;
            text-align: center;
            font-weight: bold;
        }
        @media (max-width: 768px) {
            .controls {
                flex-direction: column;
                align-items: stretch;
            }
            table {
                font-size: 10px;
            }
            th, td {
                padding: 6px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Nifty 200 Pivot Breakouts</h1>
        <div class="status-bar" id="statusBar">Mode: Live</div>
        <div class="controls">
            <input type="text" id="search" placeholder="Search by symbol..." onkeyup="filterTable()">
            <select id="sectorFilter" onchange="filterTable()">
                <option value="all">All Sectors</option>
                {% for sector in sectors %}
                <option value="{{ sector }}">{{ sector }}</option>
                {% endfor %}
            </select>
            <div>
                <label class="toggle-switch">
                    <input type="checkbox" id="filterToggle" checked onchange="filterTable()">
                    <span class="slider"></span>
                </label>
                <span class="toggle-label" id="filterLabel">All Stocks</span>
            </div>
            <div>
                <label class="toggle-switch">
                    <input type="checkbox" id="modeToggle" checked onchange="updateScanMode()">
                    <span class="slider"></span>
                </label>
                <span class="toggle-label" id="modeLabel">Live Scan</span>
            </div>
            <input type="date" id="historicalDate" style="display: none;">
            <button onclick="updateMarketData()">Refresh Now</button>
        </div>
        <table id="marketTable">
            <thead>
                <tr>
                    <th class="tooltip" data-tooltip="Stock Symbol">Symbol</th>
                    <th class="tooltip" data-tooltip="Sector">Sector</th>
                    <th class="tooltip" data-tooltip="Latest 5-min Candle Close">Close (₹)</th>
                    <th class="tooltip" data-tooltip="Latest 5-min Candle High">High (₹)</th>
                    <th class="tooltip" data-tooltip="Latest 5-min Candle Low">Low (₹)</th>
                    <th class="tooltip" data-tooltip="Pivot Point (Reference)">P (₹)</th>
                    <th class="tooltip" data-tooltip="First Resistance">R1 (₹)</th>
                    <th class="tooltip" data-tooltip="Second Resistance">R2 (₹)</th>
                    <th class="tooltip" data-tooltip="Third Resistance">R3 (₹)</th>
                    <th class="tooltip" data-tooltip="Fourth Resistance">R4 (₹)</th>
                    <th class="tooltip" data-tooltip="Fifth Resistance">R5 (₹)</th>
                    <th class="tooltip" data-tooltip="First Support">S1 (₹)</th>
                    <th class="tooltip" data-tooltip="Second Support">S2 (₹)</th>
                    <th class="tooltip" data-tooltip="Third Support">S3 (₹)</th>
                    <th class="tooltip" data-tooltip="Fourth Support">S4 (₹)</th>
                    <th class="tooltip" data-tooltip="Fifth Support">S5 (₹)</th>
                    <th class="tooltip" data-tooltip="Opening Range High (9:15-9:20 AM)">OR High (₹)</th>
                    <th class="tooltip" data-tooltip="Opening Range Low (9:15-9:20 AM)">OR Low (₹)</th>
                    <th class="tooltip" data-tooltip="Current Breakout Level">Breaking Level</th>
                    <th class="tooltip" data-tooltip="Breakout Direction">Breakout Type</th>
                    <th class="tooltip" data-tooltip="Time of Latest Candle">Candle Time</th>
                    <th class="tooltip" data-tooltip="Time of Breakout Confirmation">Breakout Time</th>
                    <th class="tooltip" data-tooltip="Breakout Status">Status</th>
                </tr>
            </thead>
            <tbody id="marketBody">
                <!-- Data will be populated by JavaScript -->
            </tbody>
        </table>
    </div>

    <script>
        let sectors = {{ sectors | tojson }};

        function formatTimestamp(timestamp) {
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
            fetch('/live-data')
                .then(response => response.json())
                .then(response => {
                    const data = response.data;
                    const mode = response.mode;
                    const date = response.date;
                    sectors = response.sectors;
                    updateSectorDropdown();
                    const statusBar = document.getElementById('statusBar');
                    statusBar.textContent = mode === 'live' ? 'Mode: Live' : `Mode: Historical (Date: ${date})`;
                    const tbody = document.getElementById('marketBody');
                    tbody.innerHTML = '';
                    for (const [symbol, info] of Object.entries(data)) {
                        const row = document.createElement('tr');
                        row.innerHTML = `
                            <td>${symbol}</td>
                            <td>${info.sector}</td>
                            <td>${info.close.toFixed(2)}</td>
                            <td>${info.high.toFixed(2)}</td>
                            <td>${info.low.toFixed(2)}</td>
                            <td>${info.p.toFixed(2)}</td>
                            <td>${info.r1.toFixed(2)}</td>
                            <td>${info.r2.toFixed(2)}</td>
                            <td>${info.r3.toFixed(2)}</td>
                            <td>${info.r4.toFixed(2)}</td>
                            <td>${info.r5.toFixed(2)}</td>
                            <td>${info.s1.toFixed(2)}</td>
                            <td>${info.s2.toFixed(2)}</td>
                            <td>${info.s3.toFixed(2)}</td>
                            <td>${info.s4.toFixed(2)}</td>
                            <td>${info.s5.toFixed(2)}</td>
                            <td>${info.opening_high.toFixed(2)}</td>
                            <td>${info.opening_low.toFixed(2)}</td>
                            <td>${info.breaking_level}</td>
                            <td class="${info.breaking_type === 'Long' ? 'long' : info.breaking_type === 'Short' ? 'short' : ''}">${info.breaking_type}</td>
                            <td>${formatTimestamp(info.timestamp)}</td>
                            <td>${info.breakout_timestamp === '-' ? '-' : formatTimestamp(info.breakout_timestamp)}</td>
                            <td class="${info.status === 'Confirmed' ? 'confirmed' : ''}">${info.status === 'Confirmed' ? '✔ Confirmed' : '-'}</td>
                        `;
                        row.dataset.symbol = symbol.toLowerCase();
                        row.dataset.status = info.status;
                        row.dataset.sector = info.sector;
                        tbody.appendChild(row);
                    }
                    filterTable();
                })
                .catch(error => {
                    console.error('Error fetching data:', error);
                    document.getElementById('statusBar').textContent = 'Error fetching data';
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