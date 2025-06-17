# extractor.py
# Purpose: Extracts logs from .tgz files in rawlogs/ and saves them in cleanlogs/ with detailed logging.

import re
import sys
import atexit
import signal
import tarfile
import shutil
import logging
from pathlib import Path
from datetime import datetime

RAW_LOGS_DIR = Path("rawlogs")
CLEAN_LOGS_DIR = Path("cleanlogs")
TEMP_DIR = Path("tmp")

# Ensure output and results folders exist before logging
CLEAN_LOGS_DIR.mkdir(exist_ok=True)
Path(__file__).parent.joinpath("results").mkdir(exist_ok=True)
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = Path(__file__).parent / "results" / f"extractor_{timestamp_str}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode='w'),
        logging.StreamHandler()
    ]
)

# Parses file names to extract airlines, flights, and all the way to the LRU
def parse_filename(filename: str):
    match = re.match(r"SECURELOGS_(\d{14})_([A-Z0-9]{3})_([A-Z0-9-]+)_([A-Z0-9]+)\.tgz", filename, re.IGNORECASE)
    if match:
        return match.groups()
    logging.warning(f"Filename format did not match expected pattern: {filename}")
    return None, None, None, None

def extract_inner_logs(base_path: Path, dest_dir: Path, timestamp: str, airline: str, tail: str, flight: str):
    count = 0
    name_counter = {}
    for inner_tgz_file in base_path.rglob("*.tgz"):
        try:
            system_match = re.search(r'securityLogs_([A-Za-z0-9]+)', inner_tgz_file.name, re.IGNORECASE)
            if system_match:
                system_name = system_match.group(1)
            else:
                system_name = "UNKNOWN_SYSTEM"
            with tarfile.open(inner_tgz_file, "r:gz") as archive:
                for member in archive.getmembers():
                    if member.isfile():
                        extracted_file = archive.extractfile(member)
                        if extracted_file:
                            log_type = Path(member.name).stem
                            base_name = f"{system_name}_{log_type}_{timestamp}_{airline}_{tail}_{flight}"
                            name_counter.setdefault(base_name, 0)
                            name_counter[base_name] += 1
                            if name_counter[base_name] > 1:
                                unique_name = f"{base_name}_{name_counter[base_name]}.log"
                            else:
                                unique_name = f"{base_name}.log"
                            output_path = dest_dir / unique_name
                            if output_path.exists():
                                logging.info(f"Skipped existing log: {output_path.name}")
                                continue
                            with open(output_path, "wb") as f:
                                shutil.copyfileobj(extracted_file, f)
                            logging.info(f"Saved log: {output_path.name}")
                            count += 1
        except Exception as e:
            logging.warning(f"Failed to extract inner tgz {inner_tgz_file.name}: {e}")
    if count == 0:
        logging.warning(f"No new inner tgz logs found under {base_path}")

# This is the actual function that extracts the logs from tgz archives
def extract_tgz_file(tgz_file: Path):
    timestamp, airline, tail, flight = parse_filename(tgz_file.name)
    if not all([timestamp, airline, tail, flight]):
        logging.warning(f"Filename format not recognized: {tgz_file.name}")
        return
    tmp_path = TEMP_DIR / tgz_file.stem
    try:
        if tmp_path.exists():
            shutil.rmtree(tmp_path)
        tmp_path.mkdir(parents=True)
        with tarfile.open(tgz_file, "r:gz") as outer:
            outer.extractall(tmp_path)
        tar_files = list(tmp_path.rglob("*.tar"))
        if tar_files:
            for tar_file in tar_files:
                inner_dir = tar_file.parent / (tar_file.stem + "_untarred")
                inner_dir.mkdir(exist_ok=True)
                with tarfile.open(tar_file, "r") as inner_tar:
                    inner_tar.extractall(inner_dir)
                extract_inner_logs(inner_dir, CLEAN_LOGS_DIR, timestamp, airline, tail, flight)
        else:
            extract_inner_logs(tmp_path, CLEAN_LOGS_DIR, timestamp, airline, tail, flight)
    except Exception as e:
        logging.warning(f"Failed to process {tgz_file.name}: {e}")
    finally:
        if tmp_path.exists():
            shutil.rmtree(tmp_path, ignore_errors=True)

def cleanup_temp_dir():
    try:
        if TEMP_DIR.exists():
            shutil.rmtree(TEMP_DIR)
            logging.info(f"Cleaned up temp folder: {TEMP_DIR}")
    except Exception as cleanup_error:
        logging.warning(f"Could not clean up temp folder {TEMP_DIR}: {cleanup_error}")

atexit.register(cleanup_temp_dir)

def handle_exit(signum, frame):
    logging.info("Received exit signal, cleaning up and exiting.")
    cleanup_temp_dir()
    sys.exit(0)

signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

def main():
    tgz_files = list(RAW_LOGS_DIR.rglob("*.tgz"))
    if not tgz_files:
        logging.info("No .tgz files found.")
        return
    for tgz_file in tgz_files:
        logging.info(f"Processing: {tgz_file.relative_to(RAW_LOGS_DIR)}")
        extract_tgz_file(tgz_file)
    # Final clean-up (optional here, already done by atexit and signal)
    cleanup_temp_dir()

if __name__ == "__main__":
    main()