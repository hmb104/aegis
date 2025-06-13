import os
import tarfile
from pathlib import Path
import shutil
import re
import logging

# Constants
RAW_LOGS_DIR = Path("rawlogs")
CLEAN_LOGS_DIR = Path("cleanlogs")
CLEAN_LOGS_DIR.mkdir(exist_ok=True)

# Setup logging
LOG_FILE = Path(__file__).parent / "extractor.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Regex pattern to parse SECURELOGS filename
SECURELOGS_PATTERN = re.compile(
    r"SECURELOGS_(\d{14})_([A-Z]{3})_([A-Z0-9\-]+)_([A-Z0-9]+)\.tgz"
)

# Helper: Extract .tgz file
def extract_tgz(tgz_path, output_dir):
    with tarfile.open(tgz_path, "r:gz") as tgz_ref:
        tgz_ref.extractall(output_dir)

# Helper: Extract .tar file
def extract_tar(tar_path, output_dir):
    with tarfile.open(tar_path, "r:") as tar_ref:
        tar_ref.extractall(output_dir)

# Phase 1: Traverse rawlogs/ recursively and process SECURELOGS_*.tgz
def process_all_logs():
    tgz_files = list(RAW_LOGS_DIR.rglob("SECURELOGS_*.tgz"))
    if not tgz_files:
        logging.warning("No SECURELOGS_*.tgz files found in rawlogs/ directory tree.")
    for tgz_file in tgz_files:
        match = SECURELOGS_PATTERN.match(tgz_file.name)
        if not match:
            logging.warning(f"Skipping unrecognized filename format: {tgz_file.name}")
            continue

        timestamp, airline_id, tail_number, flight_id = match.groups()
        working_dir = CLEAN_LOGS_DIR / f"tmp_{tgz_file.stem}"
        working_dir.mkdir(exist_ok=True)

        try:
            logging.info(f"Processing: {tgz_file.relative_to(RAW_LOGS_DIR)}")

            # Step 1: Extract outer .tgz
            extract_tgz(tgz_file, working_dir)

            # Step 2: Find and extract inner .tar
            tar_file = next(working_dir.glob("*.tar"), None)
            if not tar_file:
                logging.warning(f"No .tar found inside {tgz_file.name}")
                continue

            inner_dir = working_dir / "inner"
            inner_dir.mkdir(exist_ok=True)
            extract_tar(tar_file, inner_dir)

            # Step 3: Look for inner .tgz (real logs)
            for inner_tgz in inner_dir.glob("securityLogs_*.tgz"):
                logging.info(f"Extracting inner tgz: {inner_tgz.name}")
                inner_extract_dir = working_dir / inner_tgz.stem
                inner_extract_dir.mkdir(exist_ok=True)
                extract_tgz(inner_tgz, inner_extract_dir)

                for log_file in inner_extract_dir.glob("*.log"):
                    new_name = f"{log_file.stem}_{timestamp}_{airline_id}_{tail_number}_{flight_id}.log"
                    destination = CLEAN_LOGS_DIR / new_name
                    shutil.copy(log_file, destination)
                    logging.info(f"Saved: {destination.name}")

        except Exception as e:
            logging.error(f"Error processing {tgz_file.name}: {e}")
        finally:
            shutil.rmtree(working_dir)

if __name__ == "__main__":
    process_all_logs()
    logging.info("Log extraction complete. Clean logs saved in cleanlogs/")