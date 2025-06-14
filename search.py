import os
import re
import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# Directories
CLEAN_LOGS_DIR = Path("cleanlogs")
KEYWORDS_FILE = Path("keywords.file")

# Timestamped log file
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = Path(__file__).parent / f"search_{timestamp_str}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

def load_keywords():
    if not KEYWORDS_FILE.exists():
        logging.error(f"Keywords file not found: {KEYWORDS_FILE}")
        return []
    with open(KEYWORDS_FILE, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

def parse_log_filename(filename):
    # Supports audit.log_20220330222001_QTR_A7-ANK_QR713.log and similar
    match = re.match(
        r"([A-Za-z0-9\.]+)_(\d{14})_([A-Z0-9]{3})_([A-Z0-9-]+)_([A-Z0-9]+)\.log",
        filename,
        re.IGNORECASE
    )
    if match:
        logtype, dt, airline, tail, flight = match.groups()
        return {
            "logtype": logtype,
            "datetime": dt,
            "airline": airline,
            "tail": tail,
            "flight": flight
        }
    return {
        "logtype": "UNKNOWN",
        "datetime": "UNKNOWN",
        "airline": "UNKNOWN",
        "tail": "UNKNOWN",
        "flight": "UNKNOWN"
    }

def search_file(log_file, keywords):
    results = []
    info = parse_log_filename(log_file.name)
    try:
        with open(log_file, 'r', errors='ignore') as f:
            for line_number, line in enumerate(f, 1):
                for keyword in keywords:
                    if re.search(re.escape(keyword), line, re.IGNORECASE):
                        result_text = (
                            f"Keyword found: {keyword}\n"
                            f"File: {log_file.name}\n"
                            f"DateTime: {info['datetime']}\n"
                            f"Airline: {info['airline']}\n"
                            f"Tail: {info['tail']}\n"
                            f"Flight: {info['flight']}\n"
                            f"Line {line_number}: {line.rstrip()}\n"
                            "=========="
                        )
                        results.append(result_text)
    except Exception as e:
        results.append(f"Could not read file {log_file.name}: {e}")
    return log_file.name, results

def search_logs():
    keywords = load_keywords()
    if not keywords:
        logging.warning("No keywords to search.")
        return

    log_files = list(CLEAN_LOGS_DIR.glob("*.log"))
    if not log_files:
        logging.warning("No log files found in cleanlogs directory.")
        return

    with ThreadPoolExecutor() as executor:
        future_to_log = {executor.submit(search_file, log_file, keywords): log_file for log_file in log_files}
        for future in as_completed(future_to_log):
            log_file_name, matches = future.result()
            logging.info(f"Searching file: {log_file_name}")
            if matches:
                for m in matches:
                    logging.info("\n" + m)
            else:
                logging.info(f"No matches found in {log_file_name}")

if __name__ == "__main__":
    search_logs()
