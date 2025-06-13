import tarfile
from pathlib import Path
import shutil
import logging

# Setup paths
RAW_LOGS_DIR = Path("rawlogs")
CLEAN_LOGS_DIR = Path("cleanlogs")
TEMP_DIR = Path("temp_workspace")
LOG_FILE = Path(__file__).parent / "extractor.log"

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def extract_inner_tgz(from_path: Path, dest_dir: Path, prefix: str):
    try:
        for member in from_path.rglob("*.tgz"):
            extracted_name = f"{prefix}__{member.name}"
            output_path = dest_dir / extracted_name
            shutil.copyfile(member, output_path)
            logging.info(f"Extracted inner tgz: {output_path.name}")
    except Exception as e:
        logging.warning(f"Could not extract inner tgz from {from_path.name}: {e}")

def extract_tgz_file(tgz_file: Path):
    prefix = tgz_file.stem  # SECURELOGS_...
    tmp_path = TEMP_DIR / prefix

    try:
        if tmp_path.exists():
            shutil.rmtree(tmp_path)
        tmp_path.mkdir(parents=True)

        with tarfile.open(tgz_file, "r:gz") as outer:
            outer.extractall(tmno, p_path)

        tar_files = list(tmp_path.glob("*.tar"))
        if tar_files:
            for tar_file in tar_files:
                extract_path = tmp_path / "inner"
                with tarfile.open(tar_file, "r") as inner_tar:
                    inner_tar.extractall(extract_path)
                extract_inner_tgz(extract_path, CLEAN_LOGS_DIR, prefix)
        else:
            dot_dir = tmp_path / "."
            if dot_dir.exists():
                extract_inner_tgz(dot_dir, CLEAN_LOGS_DIR, prefix)
            else:
                extract_inner_tgz(tmp_path, CLEAN_LOGS_DIR, prefix)
    except Exception as e:
        logging.warning(f"Failed to process {tgz_file.name}: {e}")

def main():
    CLEAN_LOGS_DIR.mkdir(exist_ok=True)
    TEMP_DIR.mkdir(exist_ok=True)
    tgz_files = list(RAW_LOGS_DIR.rglob("*.tgz"))
    if not tgz_files:
        logging.info("No .tgz files found.")
        return

    for tgz_file in tgz_files:
        logging.info(f"Processing: {tgz_file.relative_to(RAW_LOGS_DIR)}")
        extract_tgz_file(tgz_file)

if __name__ == "__main__":
    main()