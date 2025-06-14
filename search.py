import os
import re
import logging
from pathlib import Path
from datetime import datetime

# Directories
CLEAN_LOGS_DIR = Path("cleanlogs")
KEYWORDS_FILE = Path("keywords.db")

# Timestamped log file
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = Path(__file__).parent / f"search_{timestamp_str}.log"

# Configure logging
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
        return [line.strip() for line in f if line.strip()]

def search_logs():
    keywords = load_keywords()
    if not keywords:
        logging.warning("No keywords to search.")
        return

    log_files = list(CLEAN_LOGS_DIR.glob("*.log"))
    if not log_files:
        logging.warning("No log files found in cleanlogs directory.")
        return

    for log_file in log_files:
        logging.info(f"Searching file: {log_file.name}")
        match_found = False
        try:
            with open(log_file, 'r', errors='ignore') as f:
                for line_number, line in enumerate(f, 1):
                    for keyword in keywords:
                        if re.search(re.escape(keyword), line, re.IGNORECASE):
                            logging.info(f"Match found: '{keyword}' in {log_file.name} (line {line_number})")
                            match_found = True
            if not match_found:
                logging.info(f"No matches found in {log_file.name}")
        except Exception as e:
            logging.warning(f"Could not read file {log_file.name}: {e}")

if __name__ == "__main__":
    search_logs()