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
import json

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
prev_candle_store = {}  # Store previous candle data
scanner_thread = None
history_data = []
last_session_date = None

# Load history data on startup
try:
    with open('history_data.json', 'r') as f:
        history_data = json.load(f)
    print(f"✅ Loaded {len(history_data)} history entries from history_data.json")
except FileNotFoundError:
    history_data = []
    print("ℹ️ No previous history data found, starting fresh")

# Save history data to JSON
def save_history_to_json():
    global history_data
    try:
        with open('history_data.json', 'w') as f:
            json.dump(history_data, f)
        logging.info(f"Saved history data to history_data.json with {len(history_data)} records")
        print(f"💾 Saved history data to history_data.json")
    except Exception as e:
        logging.error(f"Error saving history to history_data.json: {e}")
        print(f"❌ Error saving history to history_data.json: {e}")

# Check and clear history for new trading session
def check_and_clear_history():
    global history_data, last_session_date
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    current_date = now.date()
    current_time = now.time()

    # Trading session starts at 9:15 AM IST, Monday to Friday
    is_trading_day = current_date.weekday() < 5  # 0-4 is Monday-Friday
    is_after_session_start = current_time >= datetime.time(9, 15)

    if is_trading_day and is_after_session_start and last_session_date != current_date:
        history_data = []
        last_session_date = current_date
        save_history_to_json()
        logging.info(f"Cleared history data for new trading session on {current_date}")
        print(f"🧹 Cleared history data for new trading session on {current_date}")

# Check if market is open
def is_market_open():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    current_time = now.time()
    current_day = now.weekday()
    
    # Market hours: 9:15 AM to 3:30 PM IST, Monday to Friday
    market_open = datetime.time(9, 15)
    market_close = datetime.time(15, 30)
    is_trading_day = current_day < 5  # Monday to Friday
    is_within_hours = market_open <= current_time <= market_close
    
    # Placeholder for holiday check (not implemented)
    is_holiday = False  # Replace with actual holiday list check if available
    
    return is_trading_day and is_within_hours and not is_holiday

# Generate and serve live CSV for download
def generate_csv():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    cutoff_time = now.replace(hour=15, minute=30, second=0, microsecond=0)  # 3:30 PM IST
    print(f"Debug: Current time (IST): {now}, Cutoff time (IST): {cutoff_time}, Is after cutoff: {now >= cutoff_time}")

    if now < cutoff_time:
        logging.warning(f"CSV generation attempted before 3:30 PM IST: {now}")
        print(f"⚠️ CSV generation attempted before 3:30 PM IST: {now}")
        return None

    if not live_data_store:
        logging.error(f"No live data available for CSV generation at {now}, live_data_store length: {len(live_data_store)}")
        print(f"❌ No live data available for CSV generation at {now}, live_data_store length: {len(live_data_store)}")
        return b"No data available for this period"

    today = now.strftime("%Y-%m-%d")
    csv_data = "Timestamp,Symbol,Sector,Breaking Level,Breakout Type,Breakout Time,Status\n"
    confirmed_count = 0
    try:
        for symbol, data in live_data_store.items():
            if data.get("status") == "Confirmed":
                csv_data += f"{data['timestamp']},{symbol},{data.get('sector', '-')},{data.get('breaking_level', '-')},{data.get('breaking_type', '-')},{data.get('breakout_timestamp', '-')},{data.get('status', '-')}\n"
                confirmed_count += 1
        if confirmed_count == 0:
            logging.warning(f"No confirmed breakouts found for CSV generation at {now}, total entries checked: {len(live_data_store)}")
            print(f"⚠️ No confirmed breakouts found for CSV generation at {now}, total entries checked: {len(live_data_store)}")
            return b"No confirmed breakouts available"
        logging.info(f"Generated live CSV with {confirmed_count} confirmed breakouts at {now}")
        print(f"✅ Generated live CSV with {confirmed_count} confirmed breakouts at {now}")
        return csv_data.encode('utf-8')
    except Exception as e:
        logging.error(f"Error generating live CSV at {now}: {e}")
        print(f"❌ Error generating live CSV at {now}: {e}")
        return b"Error generating CSV file"

# Generate and serve history CSV for download
def generate_history_csv():
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    if not history_data:
        logging.error(f"No history data available for CSV generation at {now}, history_data length: {len(history_data)}")
        print(f"❌ No history data available for CSV generation at {now}, history_data length: {len(history_data)}")
        return b"No history data available"

    csv_data = "Timestamp,Symbol,Sector,Breaking Level,Breakout Type,Breakout Time,Status\n"
    confirmed_count = 0
    try:
        for entry in history_data:
            if not entry.get("timestamp") or not entry.get("data"):
                logging.warning(f"Invalid history entry skipped at {now}: {entry}")
                continue
            for symbol, data in entry["data"].items():
                if data.get("status") == "Confirmed":
                    csv_data += f"{entry['timestamp']},{symbol},{data.get('sector', '-')},{data.get('breaking_level', '-')},{data.get('breaking_type', '-')},{data.get('breakout_timestamp', '-')},{data.get('status', '-')}\n"
                    confirmed_count += 1
        if confirmed_count == 0:
            total_entries = sum(len(entry['data']) for entry in history_data if entry.get('data'))
            logging.warning(f"No confirmed breakouts found for history CSV generation at {now}, total entries checked: {total_entries}")
            print(f"⚠️ No confirmed breakouts found for history CSV generation at {now}, total entries checked: {total_entries}")
            return b"No confirmed breakouts available"
        logging.info(f"Generated history CSV with {confirmed_count} confirmed breakouts at {now}")
        print(f"✅ Generated history CSV with {confirmed_count} confirmed breakouts at {now}")
        return csv_data.encode('utf-8')
    except Exception as e:
        logging.error(f"Error generating history CSV at {now}: {e}")
        print(f"❌ Error generating history CSV at {now}: {e}")
        return b"Error generating history CSV file"

@app.route('/download-csv')
def download_csv():
    csv_content = generate_csv()
    print(f"Debug: download_csv at {datetime.datetime.now(pytz.timezone('Asia/Kolkata'))} - Generated content length: {len(csv_content) if csv_content else 0} bytes, Content preview: {csv_content[:50] if csv_content else 'None'}")
    if csv_content is None:
        return "CSV download available only after 3:30 PM IST", 403
    elif csv_content.startswith(b"No data") or csv_content.startswith(b"Error"):
        return csv_content.decode('utf-8'), 400
    return app.response_class(
        csv_content,
        mimetype='text/csv',
        headers={"Content-Disposition": f"attachment;filename=scan_{datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')}.csv"}
    )

@app.route('/download-history-csv')
def download_history_csv():
    csv_content = generate_history_csv()
    print(f"Debug: download_history_csv at {datetime.datetime.now(pytz.timezone('Asia/Kolkata'))} - Generated content length: {len(csv_content) if csv_content else 0} bytes, Content preview: {csv_content[:50] if csv_content else 'None'}")
    if csv_content.startswith(b"No data") or csv_content.startswith(b"Error"):
        return csv_content.decode('utf-8'), 400
    return app.response_class(
        csv_content,
        mimetype='text/csv',
        headers={"Content-Disposition": f"attachment;filename=history_{datetime.datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y-%m-%d')}.csv"}
    )

@app.route('/logs', methods=['GET'])
def get_logs():
    log_file = "nifty200_pivot_scan.log"
    try:
        if not os.path.exists(log_file):
            logging.warning("Log file not found")
            return "Log file not found", 404
        with open(log_file, 'r') as f:
            logs = f.read()
        return logs, 200, {'Content-Type': 'text/plain'}
    except Exception as e:
        logging.error(f"Error reading log file: {e}")
        return f"Error reading log file: {e}", 500

@app.route('/clear-logs', methods=['POST'])
def clear_logs():
    log_file = "nifty200_pivot_scan.log"
    try:
        with open(log_file, 'w') as f:
            f.write("")
        logging.info("Log file cleared by user")
        return jsonify({"message": "Logs cleared successfully"}), 200
    except Exception as e:
        logging.error(f"Error clearing log file: {e}")
        return jsonify({"error": f"Error clearing log file: {e}"}), 500

@app.route('/live-data', methods=['GET'])
def get_live_data():
    ist = pytz.timezone("Asia/Kolkata")
    last_updated = datetime.datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    market_open = is_market_open()
    if not market_open:
        return jsonify({
            "data": live_data_store,
            "sectors": available_sectors,
            "last_updated": last_updated,
            "market_open": False
        })
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
    return jsonify({
        "data": sorted_dict,
        "sectors": available_sectors,
        "last_updated": last_updated,
        "market_open": True
    })

@app.route('/history-data', methods=['GET'])
def get_history_data():
    ist = pytz.timezone("Asia/Kolkata")
    last_updated = datetime.datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    return jsonify({"history": history_data, "last_updated": last_updated})

# Fetch Previous Day's Data
def fetch_prev_day_data(token):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    prev_day = now - datetime.timedelta(days=1)
    two_days_ago = now - datetime.timedelta(days=2)
    
    params = {
        "exchange": "NSE",
        "symboltoken": token,
        "interval": "ONE_DAY",
        "fromdate": two_days_ago.strftime("%Y-%m-%d 09:15"),
        "todate": prev_day.strftime("%Y-%m-%d 15:30"),
    }
    print(f"Debug: Attempting fetch_prev_day_data for token {token}")
    try:
        response = smartApi.getCandleData(params)
        print(f"Debug: fetch_prev_day_data response for token {token}, attempt 1: {response}")
        if response.get("status") and response["data"]:
            df = pd.DataFrame(response["data"], columns=["timestamp", "open", "high", "low", "close", "volume"])
            if not df.empty:
                candle_datetime = datetime.datetime.strptime(df.iloc[-1]["timestamp"], "%Y-%m-%dT%H:%M:%S%z")
                candle_date = candle_datetime.date()
                print(f"Debug: Parsed date for token {token}: {candle_date}, Expected: {prev_day.date()}")
                if candle_date == prev_day.date():
                    return df.iloc[-1]
                logging.warning(f"Token {token} - Returned data does not match previous day: {candle_date} vs {prev_day.date()}")
        elif response.get("status") is False and response.get("errorcode") == "AB1004":
            logging.warning(f"Token {token} - API error AB1004: {response.get('message')}")
        else:
            logging.warning(f"Token {token} - No data received: {response.get('message', 'No data')}")
    except Exception as e:
        logging.error(f"Token {token} - Prev Day Error: {e}")
        print(f"Debug: fetch_prev_day_data error for token {token}: {e}")
    logging.warning(f"Token {token} - Failed to fetch prev day data after 1 attempt")
    print(f"⚠️ Token {token} - Failed to fetch prev day data after 1 attempt")
    return None

# Fetch 5-Minute Opening Range (9:15–9:20 AM IST)
def fetch_opening_range(token):
    ist = pytz.timezone("Asia/Kolkata")
    today = datetime.datetime.now(ist).date()
    start_time = datetime.datetime(today.year, today.month, today.day, 9, 15, tzinfo=ist)
    end_time = datetime.datetime(today.year, today.month, today.day, 9, 20, tzinfo=ist)
    
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
def fetch_latest_candle(symbol, token):
    ist = pytz.timezone("Asia/Kolkata")
    now = datetime.datetime.now(ist)
    start_dt = now - datetime.timedelta(minutes=10)  # Fetch last 10 minutes to get previous candle
    
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

# Initialize Pivot Points and Opening Range
def initialize_pivot_points_and_range():
    pivot_points = {}
    opening_ranges = {}
    logging.info(f"Starting pivot and range calculation for {len(nifty_200_stocks)} stocks")
    for idx, (symbol, data) in enumerate(nifty_200_stocks.items()):
        token = data["token"]
        if not token or not token.isdigit():
            logging.warning(f"{symbol}: Invalid token '{token}' - Skipping")
            continue
        logging.info(f"Processing {symbol} (token: {token}) - {idx + 1}/{len(nifty_200_stocks)}")
        prev_day = fetch_prev_day_data(token)
        if prev_day is None:
            logging.warning(f"{symbol}: Failed to fetch previous day data, skipping to next stock")
            continue
        opening_range = fetch_opening_range(token)
        if opening_range is None:
            logging.warning(f"{symbol}: Failed to fetch opening range")
            continue
        pivot_points[symbol] = calculate_pivots(prev_day)
        opening_ranges[symbol] = opening_range
        logging.info(f"{symbol} Pivot Levels: {pivot_points[symbol]}, Opening Range: {opening_range}")
        print(f"✅ {symbol} Pivot Levels and Opening Range Calculated")
        time.sleep(1.0)  # Small delay to avoid overwhelming API
    logging.info(f"Completed calculation. Processed {len(pivot_points)} stocks")
    return pivot_points, opening_ranges

# Save live_data_store to CSV
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

# Save confirmed breakouts to history
def save_to_history():
    global history_data
    ist = pytz.timezone("Asia/Kolkata")
    timestamp = datetime.datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
    confirmed_data = {symbol: dict(live_data_store[symbol]) for symbol in live_data_store if live_data_store[symbol].get("status") == "Confirmed"}
    if not confirmed_data:
        print(f"Debug: No confirmed breakouts to save at {timestamp}")
        return
    history_snapshot = {"timestamp": timestamp, "data": confirmed_data}
    history_data.append(history_snapshot)
    logging.info(f"Saved scan history at {timestamp} with {len(confirmed_data)} confirmed records")
    print(f"💾 Saved scan history at {timestamp} with {len(confirmed_data)} confirmed records")
    save_history_to_json()

# Live Market Scanner
def live_market_scan():
    global live_data_store, active_breakouts, prev_candle_store
    print("Starting live market scan...")
    pivot_points, opening_ranges = initialize_pivot_points_and_range()
    if not pivot_points or not opening_ranges:
        logging.error("Initialization failed, no pivot points or opening ranges")
        print("❌ Initialization failed, no pivot points or opening ranges")
        return
    last_candle_time = {symbol: None for symbol in nifty_200_stocks}
    ist = pytz.timezone("Asia/Kolkata")
    print("Debug: Initiation complete, entering live scan loop")

    while threading.current_thread().is_alive():
        check_and_clear_history()
        scan_time = datetime.datetime.now(ist).strftime("%Y-%m-%d %H:%M:%S")
        print(f"Scanning at {scan_time}")
        for symbol, data in nifty_200_stocks.items():
            token = data["token"]
            if not token or not token.isdigit():
                continue
            if symbol not in pivot_points or symbol not in opening_ranges:
                continue
            candles = fetch_latest_candle(symbol, token)
            if candles is None or len(candles) < 1:
                print(f"Debug: fetch_latest_candle failed for {symbol}")
                continue

            current_candle = candles.iloc[-1]
            prev_candle = candles.iloc[-2] if len(candles) >= 2 else None
            timestamp = current_candle["timestamp"]
            if last_candle_time[symbol] == timestamp:
                continue
            last_candle_time[symbol] = timestamp

            close, high, low = current_candle["close"], current_candle["high"], current_candle["low"]
            prev_close = prev_candle["close"] if prev_candle is not None else None
            prev_timestamp = prev_candle["timestamp"] if prev_candle is not None else None

            pivots = pivot_points[symbol]
            opening_range = opening_ranges[symbol]
            levels = {
                "R5": pivots["R5"], "R4": pivots["R4"], "R3": pivots["R3"], "R2": pivots["R2"], "R1": pivots["R1"],
                "S1": pivots["S1"], "S2": pivots["S2"], "S3": pivots["S3"], "S4": pivots["S4"], "S5": pivots["S5"]
            }

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

            if breakout_level and prev_candle is not None:
                print(f"Debug: {symbol} - Checking breakout: prev_close={prev_close}, close={close}, level={breakout_value}")
                if breakout_type == "Long" and prev_close is not None and prev_close > breakout_value:
                    if close > breakout_value:
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
                elif breakout_type == "Short" and prev_close is not None and prev_close < breakout_value:
                    if close < breakout_value:
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

            if prev_candle is not None:
                prev_candle_store[symbol] = {
                    "close": prev_close,
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
            print(f"Debug: live_data_store[{symbol}] = {live_data_store.get(symbol, 'Not populated')}")

        save_to_history()
        save_to_csv()
        time.sleep(60)

# Flask Route for Live Market
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
            --primary: #1a73e8;
            --success: #28a745;
            --danger: #dc3545;
            --bg-light: #f0f2f5;
            --bg-dark: #1e2a44;
            --text-light: #333;
            --text-dark: #e0e0e0;
            --card-bg-light: #ffffff;
            --card-bg-dark: #2b3a5e;
            --shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
            --transition: all 0.3s ease;
        }

        [data-theme="dark"] {
            --bg-light: var(--bg-dark);
            --text-light: var(--text-dark);
            --card-bg-light: var(--card-bg-dark);
        }

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif;
            background: var(--bg-light);
            color: var(--text-light);
            line-height: 1.6;
            min-height: 100vh;
            display: flex;
            flex-direction: column;
        }

        .container {
            max-width: 1600px;
            margin: 0 auto;
            padding: 1rem;
            flex: 1;
        }

        header {
            text-align: center;
            margin-bottom: 1.5rem;
        }

        h1 {
            font-size: clamp(1.5rem, 5vw, 2rem);
            color: var(--primary);
            margin-bottom: 0.5rem;
        }

        .status-bar {
            font-size: 0.9rem;
            background: var(--card-bg-light);
            padding: 0.5rem;
            border-radius: 6px;
            box-shadow: var(--shadow);
            margin-bottom: 1rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 0.5rem;
        }

        .clock {
            font-family: monospace;
            font-size: 0.9rem;
            background: #f0f0f0;
            padding: 0.25rem 0.5rem;
            border-radius: 4px;
        }

        [data-theme="dark"] .clock {
            background: #3a4a6e;
        }

        .tab-buttons {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }

        .tab-button {
            padding: 0.5rem 1rem;
            background: var(--card-bg-light);
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9rem;
            transition: var(--transition);
        }

        .tab-button.active {
            background: var(--primary);
            color: white;
        }

        .tab-button:hover:not(:disabled) {
            background: var(--primary);
            color: white;
            opacity: 0.9;
        }

        .tab-button:disabled {
            background: #ccc;
            cursor: not-allowed;
            color: #666;
        }

        .controls {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 0.5rem;
            margin-bottom: 1rem;
        }

        .controls input,
        .controls select,
        .controls button {
            padding: 0.5rem;
            font-size: 0.9rem;
            border: 1px solid #ddd;
            border-radius: 6px;
            transition: var(--transition);
        }

        .controls input:focus,
        .controls select:focus {
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 0 2px rgba(26, 115, 232, 0.2);
        }

        .controls button {
            background: var(--primary);
            color: white;
            border: none;
            cursor: pointer;
        }

        .controls button:hover {
            background: #1557b0;
        }

        .theme-toggle {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }

        .theme-toggle input {
            display: none;
        }

        .theme-toggle label {
            width: 40px;
            height: 20px;
            background: #ccc;
            border-radius: 20px;
            position: relative;
            cursor: pointer;
            transition: var(--transition);
        }

        .theme-toggle label::after {
            content: '';
            width: 16px;
            height: 16px;
            background: white;
            border-radius: 50%;
            position: absolute;
            top: 2px;
            left: 2px;
            transition: var(--transition);
        }

        .theme-toggle input:checked + label {
            background: var(--primary);
        }

        .theme-toggle input:checked + label::after {
            transform: translateX(20px);
        }

        .table-wrapper {
            position: relative;
            overflow-x: auto;
            background: var(--card-bg-light);
            border-radius: 8px;
            box-shadow: var(--shadow);
        }

        .market-closed {
            text-align: center;
            padding: 2rem;
            font-size: 1rem;
            color: #666;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }

        th, td {
            padding: 0.75rem;
            text-align: center;
            border-bottom: 1px solid #eee;
        }

        th {
            background: var(--primary);
            color: white;
            position: sticky;
            top: 0;
            z-index: 10;
            cursor: pointer;
        }

        th.sortable:hover {
            background: #1557b0;
        }

        th.sortable::after {
            content: '↕';
            margin-left: 0.5rem;
            opacity: 0.5;
        }

        th.sort-asc::after {
            content: '↑';
            opacity: 1;
        }

        th.sort-desc::after {
            content: '↓';
            opacity: 1;
        }

        tr:nth-child(even) {
            background: #fafafa;
        }

        [data-theme="dark"] tr:nth-child(even) {
            background: #3a4a6e;
        }

        tr:hover {
            background: #f5f5f5;
        }

        [data-theme="dark"] tr:hover {
            background: #4a5a7e;
        }

        .long { color: var(--success); font-weight: 600; }
        .short { color: var(--danger); font-weight: 600; }
        .confirmed { color: var(--success); font-weight: 600; }

        .tooltip {
            position: relative;
        }

        .tooltip:hover::after {
            content: attr(data-tooltip);
            position: absolute;
            bottom: 100%;
            left: 50%;
            transform: translateX(-50%);
            background: #333;
            color: white;
            padding: 0.5rem;
            border-radius: 4px;
            font-size: 0.8rem;
            white-space: nowrap;
            z-index: 20;
        }

        .tab-content {
            display: none;
        }

        .tab-content.active {
            display: block;
        }

        .download-btn {
            margin: 1rem 0;
            padding: 0.75rem 1.5rem;
            background: var(--success);
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9rem;
            transition: var(--transition);
        }

        .download-btn:hover {
            background: #218838;
        }

        .download-btn:disabled {
            background: #ccc;
            cursor: not-allowed;
        }

        .log-viewer {
            background: var(--card-bg-light);
            border-radius: 8px;
            box-shadow: var(--shadow);
            padding: 1rem;
            max-height: 500px;
            overflow-y: auto;
            font-family: monospace;
            font-size: 0.9rem;
            white-space: pre-wrap;
            word-wrap: break-word;
        }

        [data-theme="dark"] .log-viewer {
            background: #3a4a6e;
        }

        .log-controls {
            display: flex;
            gap: 0.5rem;
            margin-bottom: 1rem;
        }

        .log-controls button {
            padding: 0.5rem 1rem;
            background: var(--primary);
            color: white;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9rem;
            transition: var(--transition);
        }

        .log-controls button:hover {
            background: #1557b0;
        }

        .log-controls button.clear {
            background: var(--danger);
        }

        .log-controls button.clear:hover {
            background: #c82333;
        }

        .loader {
            border: 4px solid #f3f3f3;
            border-top: 4px solid var(--primary);
            border-radius: 50%;
            width: 24px;
            height: 24px;
            animation: spin 1s linear infinite;
            position: absolute;
            top: 50%;
            left: 50%;
            transform: translate(-50%, -50%);
            display: none;
        }

        .loading .loader {
            display: block;
        }

        @keyframes spin {
            0% { transform: translate(-50%, -50%) rotate(0deg); }
            100% { transform: translate(-50%, -50%) rotate(360deg); }
        }

        @media (max-width: 768px) {
            .controls {
                grid-template-columns: 1fr;
            }

            table {
                font-size: 0.8rem;
            }

            th, td {
                padding: 0.5rem;
            }

            th:not(:first-child):not(:nth-child(2)),
            td:not(:first-child):not(:nth-child(2)) {
                display: none;
            }

            .table-wrapper {
                max-height: 400px;
                overflow-y: auto;
            }

            .log-viewer {
                max-height: 300px;
            }
        }

        @media (max-width: 480px) {
            h1 {
                font-size: 1.2rem;
            }

            .tab-button {
                font-size: 0.8rem;
                padding: 0.5rem;
            }

            .status-bar {
                flex-direction: column;
                gap: 0.5rem;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>Nifty 200 Pivot Breakouts</h1>
            <div class="status-bar">
                <span id="statusBar">Mode: Live</span>
                <span id="clock" class="clock"></span>
                <div class="theme-toggle">
                    <input type="checkbox" id="themeToggle">
                    <label for="themeToggle"></label>
                    <span>Dark Mode</span>
                </div>
            </div>
        </header>
        <main>
            <nav class="tab-buttons">
                <button id="liveTab" class="tab-button active" onclick="openTab('live')" data-tooltip="Live market data (9:15 AM - 3:30 PM IST, Mon-Fri)">Live</button>
                <button id="historyTab" class="tab-button" onclick="openTab('history')" data-tooltip="Historical breakout data">History</button>
                <button id="logsTab" class="tab-button" onclick="openTab('logs')" data-tooltip="View application logs">Logs</button>
            </nav>
            <section id="live-tab" class="tab-content active">
                <div class="controls">
                    <input type="text" id="search" placeholder="Search by symbol..." oninput="debounceFilter()">
                    <select id="sectorFilter" onchange="filterTable()">
                        <option value="all">All Sectors</option>
                        {% for sector in sectors %}
                        <option value="{{ sector }}">{{ sector }}</option>
                        {% endfor %}
                    </select>
                    <button onclick="updateMarketData()">Refresh Now</button>
                </div>
                <div id="marketClosed" class="market-closed" style="display: none;">
                    Market is closed. Live data available from 9:15 AM to 3:30 PM IST, Monday to Friday.
                </div>
                <div class="table-wrapper">
                    <div class="loader"></div>
                    <table id="marketTable">
                        <thead>
                            <tr>
                                <th class="sortable tooltip" data-tooltip="Stock Symbol" data-sort="symbol">Symbol</th>
                                <th class="sortable tooltip" data-tooltip="Sector" data-sort="sector">Sector</th>
                                <th class="sortable tooltip" data-tooltip="Current Breakout Level" data-sort="breaking_level">Breaking Level</th>
                                <th class="sortable tooltip" data-tooltip="Breakout Direction" data-sort="breaking_type">Breakout Type</th>
                                <th class="sortable tooltip" data-tooltip="Time of Breakout Confirmation" data-sort="breakout_timestamp">Breakout Time</th>
                                <th class="sortable tooltip" data-tooltip="Breakout Status" data-sort="status">Status</th>
                            </tr>
                        </thead>
                        <tbody id="marketBody"></tbody>
                    </table>
                </div>
                <button id="downloadLiveBtn" class="download-btn" onclick="downloadLiveCSV()" disabled>Download Live CSV</button>
            </section>
            <section id="history-tab" class="tab-content">
                <div class="controls">
                    <input type="text" id="search" placeholder="Search by symbol..." oninput="debounceFilter()">
                    <select id="sectorFilter" onchange="filterTable()">
                        <option value="all">All Sectors</option>
                        {% for sector in sectors %}
                        <option value="{{ sector }}">{{ sector }}</option>
                        {% endfor %}
                    </select>
                    <button onclick="updateHistoryData()">Refresh Now</button>
                </div>
                <div class="table-wrapper">
                    <div class="loader"></div>
                    <table id="historyTable">
                        <thead>
                            <tr>
                                <th class="sortable tooltip" data-tooltip="Scan Timestamp" data-sort="timestamp">Timestamp</th>
                                <th class="sortable tooltip" data-tooltip="Stock Symbol" data-sort="symbol">Symbol</th>
                                <th class="sortable tooltip" data-tooltip="Sector" data-sort="sector">Sector</th>
                                <th class="sortable tooltip" data-tooltip="Breakout Level" data-sort="breaking_level">Breakout Level</th>
                                <th class="sortable tooltip" data-tooltip="Breakout Direction" data-sort="breaking_type">Breakout Type</th>
                                <th class="sortable tooltip" data-tooltip="Breakout Confirmation Time" data-sort="breakout_timestamp">Breakout Time</th>
                                <th class="sortable tooltip" data-tooltip="Breakout Status" data-sort="status">Status</th>
                            </tr>
                        </thead>
                        <tbody id="historyBody"></tbody>
                    </table>
                </div>
                <button id="downloadHistoryBtn" class="download-btn" onclick="downloadHistoryCSV()">Download History CSV</button>
            </section>
            <section id="logs-tab" class="tab-content">
                <div class="log-controls">
                    <button onclick="updateLogs()">Refresh Logs</button>
                    <button class="clear" onclick="clearLogs()">Clear Logs</button>
                </div>
                <div class="log-viewer" id="logViewer">Loading logs...</div>
            </section>
        </main>
    </div>

    <script>
    let sectors = {{ sectors | tojson }};
    let activeTab = 'live';
    let sortState = { column: '', direction: '' };
    let filterTimeout;

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

    function updateClock() {
        const now = new Date().toLocaleString('en-US', {
            timeZone: 'Asia/Kolkata',
            hour: 'numeric',
            minute: 'numeric',
            second: 'numeric',
            hour12: true
        });
        document.getElementById('clock').textContent = now;
    }

    function isMarketOpen() {
        const now = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
        const date = new Date(now);
        const hours = date.getHours();
        const minutes = date.getMinutes();
        const day = date.getDay();
        const isTradingDay = day >= 1 && day <= 5; // Monday to Friday
        const isWithinHours = (hours > 9 || (hours === 9 && minutes >= 15)) && (hours < 15 || (hours === 15 && minutes <= 30));
        return isTradingDay && isWithinHours;
    }

    function updateMarketStatus() {
        const liveTab = document.getElementById('liveTab');
        const marketClosed = document.getElementById('marketClosed');
        const marketTableWrapper = document.querySelector('#live-tab .table-wrapper');
        const downloadLiveBtn = document.getElementById('downloadLiveBtn');
        
        if (isMarketOpen()) {
            liveTab.disabled = false;
            liveTab.title = '';
            marketClosed.style.display = 'none';
            marketTableWrapper.style.display = 'block';
            downloadLiveBtn.disabled = false;
        } else {
            liveTab.disabled = true;
            liveTab.title = 'Market is closed';
            marketClosed.style.display = 'block';
            marketTableWrapper.style.display = 'none';
            downloadLiveBtn.disabled = true;
            if (activeTab === 'live') {
                openTab('history');
            }
        }
    }

    function openTab(tabName) {
        if (tabName === 'live' && !isMarketOpen()) {
            return;
        }
        document.querySelectorAll('.tab-content').forEach(tab => tab.classList.remove('active'));
        document.querySelectorAll('.tab-button').forEach(btn => btn.classList.remove('active'));
        document.getElementById(`${tabName}-tab`).classList.add('active');
        document.querySelector(`.tab-button[onclick="openTab('${tabName}')"]`).classList.add('active');
        activeTab = tabName;
        sortState = { column: '', direction: '' };
        if (tabName === 'history') {
            updateHistoryData();
        } else if (tabName === 'logs') {
            updateLogs();
        } else {
            updateMarketData();
        }
    }

    function setLoading(tab) {
        const wrapper = document.querySelector(`#${tab}-tab .table-wrapper, #${tab}-tab .log-viewer`);
        if (wrapper) wrapper.classList.add('loading');
    }

    function clearLoading(tab) {
        const wrapper = document.querySelector(`#${tab}-tab .table-wrapper, #${tab}-tab .log-viewer`);
        if (wrapper) wrapper.classList.remove('loading');
    }

    function updateMarketData() {
        if (activeTab !== 'live' || !isMarketOpen()) return;
        setLoading('live');
        fetch('/live-data')
            .then(response => response.json())
            .then(response => {
                const data = response.data;
                sectors = response.sectors;
                document.getElementById('statusBar').textContent = 'Mode: Live';
                const tbody = document.getElementById('marketBody');
                tbody.innerHTML = '';
                for (const [symbol, info] of Object.entries(data)) {
                    const row = document.createElement('tr');
                    row.innerHTML = `
                        <td>${symbol}</td>
                        <td>${info.sector}</td>
                        <td>${info.breaking_level}</td>
                        <td class="${info.breaking_type === 'Long' ? 'long' : info.breaking_type === 'Short' ? 'short' : ''}">${info.breaking_type}</td>
                        <td>${info.breakout_timestamp === '-' ? '-' : formatTimestamp(info.breakout_timestamp)}</td>
                        <td class="${info.status === 'Confirmed' ? 'confirmed' : ''}">${info.status === 'Confirmed' ? '✔ Confirmed' : '-'}</td>
                    `;
                    row.dataset.symbol = symbol.toLowerCase();
                    row.dataset.sector = info.sector;
                    row.dataset.breaking_level = info.breaking_level;
                    row.dataset.breaking_type = info.breaking_type;
                    row.dataset.breakout_timestamp = info.breakout_timestamp;
                    row.dataset.status = info.status;
                    tbody.appendChild(row);
                }
                sortTable();
                filterTable();
                clearLoading('live');
            })
            .catch(error => {
                console.error('Error fetching live data:', error);
                document.getElementById('statusBar').textContent = 'Error fetching data';
                clearLoading('live');
            });
    }

    function updateHistoryData() {
        if (activeTab !== 'history') return;
        setLoading('history');
        fetch('/history-data')
            .then(response => {
                if (!response.ok) throw new Error('Network response was not ok');
                return response.json();
            })
            .then(response => {
                const history = response.history || [];
                document.getElementById('statusBar').textContent = 'Mode: History';
                const tbody = document.getElementById('historyBody');
                tbody.innerHTML = '';
                if (history.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="7">No history data available</td></tr>';
                } else {
                    history.forEach(entry => {
                        for (const [symbol, info] of Object.entries(entry["data"])) {
                            const row = document.createElement('tr');
                            row.innerHTML = `
                                <td>${formatTimestamp(entry['timestamp'])}</td>
                                <td>${symbol}</td>
                                <td>${info.sector}</td>
                                <td>${info.breaking_level}</td>
                                <td class="${info.breaking_type === 'Long' ? 'long' : info.breaking_type === 'Short' ? 'short' : ''}">${info.breaking_type}</td>
                                <td>${info.breakout_timestamp === '-' ? '-' : formatTimestamp(info.breakout_timestamp)}</td>
                                <td class="${info.status === 'Confirmed' ? 'confirmed' : ''}">${info.status === 'Confirmed' ? '✔ Confirmed' : '-'}</td>
                            `;
                            row.dataset.timestamp = entry['timestamp'];
                            row.dataset.symbol = symbol.toLowerCase();
                            row.dataset.sector = info.sector;
                            row.dataset.breaking_level = info.breaking_level;
                            row.dataset.breaking_type = info.breaking_type;
                            row.dataset.breakout_timestamp = info.breakout_timestamp;
                            row.dataset.status = info.status;
                            tbody.appendChild(row);
                        }
                    });
                }
                sortTable();
                filterTable();
                clearLoading('history');
            })
            .catch(error => {
                console.error('Error fetching history:', error);
                document.getElementById('statusBar').textContent = 'Error fetching history';
                document.getElementById('historyBody').innerHTML = '<tr><td colspan="7">Error loading history</td></tr>';
                clearLoading('history');
            });
    }

    function updateLogs() {
        if (activeTab !== 'logs') return;
        setLoading('logs');
        fetch('/logs')
            .then(response => {
                if (!response.ok) throw new Error('Failed to fetch logs');
                return response.text();
            })
            .then(logs => {
                const logViewer = document.getElementById('logViewer');
                logViewer.textContent = logs || 'No logs available';
                logViewer.scrollTop = logViewer.scrollHeight;
                clearLoading('logs');
            })
            .catch(error => {
                console.error('Error fetching logs:', error);
                document.getElementById('logViewer').textContent = 'Error loading logs';
                clearLoading('logs');
            });
    }

    function clearLogs() {
        if (!confirm('Are you sure you want to clear all logs? This action cannot be undone.')) return;
        fetch('/clear-logs', { method: 'POST' })
            .then(response => response.json())
            .then(data => {
                alert(data.message || data.error);
                updateLogs();
            })
            .catch(error => {
                console.error('Error clearing logs:', error);
                alert('Error clearing logs');
            });
    }

    function updateSectorDropdown() {
        const sectorFilter = document.getElementById('sectorFilter');
        if (!sectorFilter) return;
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

    function debounceFilter() {
        clearTimeout(filterTimeout);
        filterTimeout = setTimeout(filterTable, 300);
    }

    function filterTable() {
        if (activeTab === 'logs') return;
        const search = document.getElementById('search')?.value.toLowerCase() || '';
        const sector = document.getElementById('sectorFilter')?.value || 'all';
        const rows = document.querySelectorAll(`${activeTab === 'live' ? '#marketTable' : '#historyTable'} tbody tr`);

        rows.forEach(row => {
            const symbol = row.dataset.symbol;
            const rowSector = row.dataset.sector;
            const matchesSearch = symbol.includes(search);
            const matchesSector = sector === 'all' || rowSector === sector;
            row.style.display = matchesSearch && matchesSector ? '' : 'none';
        });
    }

    function sortTable() {
        if (activeTab === 'logs') return;
        const table = document.querySelector(`${activeTab === 'live' ? '#marketTable' : '#historyTable'}`);
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const column = sortState.column;
        const direction = sortState.direction;

        if (!column) return;

        rows.sort((a, b) => {
            let aValue = a.dataset[column] || '-';
            let bValue = b.dataset[column] || '-';

            if (column.includes('timestamp')) {
                aValue = aValue === '-' ? '9999-12-31' : aValue;
                bValue = bValue === '-' ? '9999-12-31' : bValue;
                aValue = new Date(aValue).getTime();
                bValue = new Date(bValue).getTime();
            } else {
                aValue = aValue.toLowerCase();
                bValue = bValue.toLowerCase();
            }

            if (aValue < bValue) return direction === 'asc' ? -1 : 1;
            if (aValue > bValue) return direction === 'asc' ? 1 : -1;
            return 0;
        });

        tbody.innerHTML = '';
        rows.forEach(row => tbody.appendChild(row));
    }

    function handleSort(event) {
        if (activeTab === 'logs') return;
        const th = event.target.closest('th.sortable');
        if (!th) return;
        const column = th.dataset.sort;
        if (sortState.column === column) {
            sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
        } else {
            sortState.column = column;
            sortState.direction = 'asc';
        }

        document.querySelectorAll('th.sortable').forEach(header => {
            header.classList.remove('sort-asc', 'sort-desc');
        });
        th.classList.add(`sort-${sortState.direction}`);
        sortTable();
    }

    function downloadLiveCSV() {
        window.location.href = '/download-csv';
    }

    function downloadHistoryCSV() {
        window.location.href = '/download-history-csv';
    }

    function checkLiveDownloadAvailability() {
        const now = new Date().toLocaleString('en-US', { timeZone: 'Asia/Kolkata' });
        const [date, time] = now.split(', ');
        const [hour, minute] = time.split(':');
        const downloadBtn = document.getElementById('downloadLiveBtn');
        if (parseInt(hour) >= 15 && parseInt(minute) >= 30) {
            downloadBtn.disabled = false;
        } else {
            downloadBtn.disabled = true;
        }
    }

    function toggleTheme() {
        const isDark = document.getElementById('themeToggle').checked;
        document.body.dataset.theme = isDark ? 'dark' : 'light';
        localStorage.setItem('theme', isDark ? 'dark' : 'light');
    }

    document.getElementById('themeToggle').addEventListener('change', toggleTheme);
    document.querySelectorAll('.table-wrapper').forEach(wrapper => {
        wrapper.addEventListener('click', handleSort);
    });

    if (localStorage.getItem('theme') === 'dark') {
        document.getElementById('themeToggle').checked = true;
        document.body.dataset.theme = 'dark';
    }

    updateClock();
    setInterval(updateClock, 1000);
    updateMarketStatus();
    setInterval(updateMarketStatus, 60000);
    updateMarketData();
    setInterval(() => {
        if (activeTab === 'live' && isMarketOpen()) {
            updateMarketData();
        } else if (activeTab === 'history') {
            updateHistoryData();
        } else if (activeTab === 'logs') {
            updateLogs();
        }
        checkLiveDownloadAvailability();
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
    port = int(os.getenv("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)