import os
from pathlib import Path
import logging

# Constants
CLEAN_LOGS_DIR = Path("cleanlogs")
KEYWORDS_FILE = Path("keywords.db")
OUTPUT_FILE = Path("search_results.txt")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

def load_keywords():
    if not KEYWORDS_FILE.exists():
        logging.error("keywords.db not found. Please provide one.")
        return []
    with open(KEYWORDS_FILE, "r") as f:
        return [line.strip().lower() for line in f if line.strip() and not line.startswith("#")]

def search_logs(keywords):
    matches = []
    for log_file in CLEAN_LOGS_DIR.glob("*.log"):
        try:
            with open(log_file, "r", errors="ignore") as f:
                for i, line in enumerate(f, start=1):
                    lower_line = line.lower()
                    if any(keyword in lower_line for keyword in keywords):
                        match = f"{log_file.name} [Line {i}]: {line.strip()}"
                        matches.append(match)
        except Exception as e:
            logging.warning(f"Could not read {log_file.name}: {e}")

    if matches:
        with open(OUTPUT_FILE, "w") as out:
            for match in matches:
                out.write(match + "\n")
        logging.info(f"Search complete. {len(matches)} matches written to {OUTPUT_FILE}")
    else:
        logging.info("No matches found.")

if __name__ == "__main__":
    keywords = load_keywords()
    if keywords:
        search_logs(keywords)