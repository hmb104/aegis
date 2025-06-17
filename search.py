# search.py
# Purpose: Perfomed basic search on each .log inside cleanlogs/. for specified keywords (keywords.file) and records detailed match info.

import re
import time
import logging
from tqdm import tqdm
from pathlib import Path
from datetime import datetime
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

# Setup log directories
CLEAN_LOGS_DIR = Path("cleanlogs")
KEYWORDS_FILE = Path("keywords.file")

# Generate timestamped log file
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = Path(__file__).parent / f"search_{timestamp_str}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w')
    ]
)

# Log errors to console
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

def load_keywords():
    if not KEYWORDS_FILE.exists():
        logging.error(f"Keywords file not found: {KEYWORDS_FILE}")
        return []
    with open(KEYWORDS_FILE, 'r') as f:
        return [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]

def parse_log_filename(filename):
    # Matches: DSU1_audit_20220307232056_SIA_9V-SHJ_SQ999.log
    # or     : DSU1_audit_20220307232056_SIA_9V-SHJ_SQ999_2.log
    match = re.match(
        r"([A-Za-z0-9]+)_([A-Za-z0-9\.]+)_(\d{14})_([A-Z0-9]{3})_([A-Z0-9-]+)_([A-Z0-9]+)(?:_\d+)?\.log",
        filename,
        re.IGNORECASE
    )
    if match:
        system, logtype, dt, airline, tail, flight = match.groups()
        return {
            "system": system,
            "logtype": logtype,
            "datetime": dt,
            "airline": airline,
            "tail": tail,
            "flight": flight
        }
    return {
        "system": "UNKNOWN",
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
                            "================================"
                        )
                        results.append((keyword, result_text))
    except Exception as e:
        results.append(("ERROR", f"Could not read file {log_file.name}: {e}"))
    return log_file.name, results

def analytics(total_files, total_matches, airline_counter, elapsed, keyword_counter):
    logging.info(f"Completed search of {total_files} log files in {elapsed:.2f} seconds.")
    logging.info(f"Total matches found: {total_matches}")
    if airline_counter:
        top_airlines = airline_counter.most_common(3)
        logging.info("Top airlines with matches:")
        for airline, count in top_airlines:
            logging.info(f"  {airline}: {count} matches")
    else:
        logging.info("No airline matches found.")
    if keyword_counter:
        top_keywords = keyword_counter.most_common(5)
        logging.info("Top keywords with matches:")
        for keyword, count in top_keywords:
            logging.info(f"  {keyword}: {count} matches")
    else:
        logging.info("No keyword matches found.")

def search_logs():
    keywords = load_keywords()
    if not keywords:
        logging.warning("No keywords to search.")
        return

    log_files = list(CLEAN_LOGS_DIR.glob("*.log"))
    if not log_files:
        logging.warning("No log files found in cleanlogs directory.")
        return

    start_time = time.time()
    airline_counter = Counter()
    keyword_counter = Counter()
    total_matches = 0

    with ThreadPoolExecutor() as executor:
        future_to_log = {executor.submit(search_file, log_file, keywords): log_file for log_file in log_files}
        for future in tqdm(as_completed(future_to_log), total=len(log_files), desc="Searching logs", unit="file"):
            log_file_name, matches = future.result()
            logging.info(f"Searching file: {log_file_name}")
            if matches:
                # Handle read errors separately so they don't count as matches
                if len(matches) == 1 and matches[0][0] == "ERROR":
                    logging.error(matches[0][1])
                    continue

                info = parse_log_filename(log_file_name)
                airline = info["airline"]
                airline_hits = 0
                for keyword, m in matches:
                    logging.info("\n" + m)
                    keyword_counter[keyword] += 1
                    airline_hits += 1
                if airline != "UNKNOWN":
                    airline_counter[airline] += airline_hits
                total_matches += airline_hits
            else:
                logging.info(f"No matches found in {log_file_name}")

    elapsed = time.time() - start_time
    analytics(len(log_files), total_matches, airline_counter, elapsed, keyword_counter)
    print("Search is Complete!")

if __name__ == "__main__":
    search_logs()