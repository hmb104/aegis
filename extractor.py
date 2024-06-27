"""
extractor.py

This script handles the recursive extraction of log files from offloaded log archives
"""

import tarfile
import gzip
import shutil
import os
import logging
from datetime import datetime

class LogExtractor:
    def __init__(self, raw_dir, logs_dir, logger):
        """
        Initialize the LogExtractor with directories and logger.
        """
        self.raw_dir = raw_dir
        self.logs_dir = logs_dir
        self.logger = logger
        self.success_count = 0
        self.fail_count = 0
        self.errors = []

    def extract(self):
        """
        Extract all .tgz and .gz files in the /raw directory and organize them by flight into /logs.
        """
        self.logger.debug(f"Starting extraction from raw directory: {self.raw_dir}")
        for file_name in os.listdir(self.raw_dir):
            if file_name.endswith('.tgz') or file_name.endswith('.gz'):
                self.logger.debug(f"Processing file: {file_name}")
                file_path = os.path.join(self.raw_dir, file_name)
                airline_code, tail_number, flight_number, timestamp = self._extract_file_info(file_name)
                folder_name = f"{airline_code}_{tail_number}_{flight_number}_{timestamp}"
                output_path = os.path.join(self.logs_dir, folder_name)

                if os.path.exists(output_path):
                    self.logger.info(f"This archive has already been processed. Please delete the existing folder '{output_path}' to process again.")
                    continue

                self._extract_file(file_path, output_path, airline_code, tail_number, flight_number, timestamp, level=1)
                self._log_processed_archive(file_name)
        self._write_log_summary()
        self.logger.debug("Extraction process completed.")

    def _extract_file(self, file_path, output_path, airline, tail_number, flight_number, timestamp, level):
        try:
            self.logger.debug(f"Extracting file: {file_path} at level {level} into folder {output_path}")
            os.makedirs(output_path, exist_ok=True)

            if file_path.endswith('.tgz'):
                with tarfile.open(file_path, 'r:gz') as tar:
                    for member in tar.getmembers():
                        if member.isfile():
                            member_name = os.path.basename(member.name)
                            if member_name.endswith('.tgz') or member_name.endswith('.gz'):
                                temp_path = os.path.join(output_path, member_name)
                                with open(temp_path, 'wb') as f:
                                    f.write(tar.extractfile(member).read())
                                self._extract_file(temp_path, output_path, airline, tail_number, flight_number, timestamp, level + 1)
                                os.remove(temp_path)
                            elif not (member_name.endswith('.xml') or member_name.endswith('.sig')):
                                extracted_path = os.path.join(output_path, member_name)
                                tar.extract(member, path=output_path)
                                unique_path = self._make_unique(extracted_path)
                                os.rename(extracted_path, unique_path)
                                self.logger.info(f"Extracted {member.name} to {unique_path}")
                                self.success_count += 1

            elif file_path.endswith('.gz'):
                member_name = os.path.basename(file_path)[:-3]
                temp_path = os.path.join(output_path, member_name)
                with gzip.open(file_path, 'rb') as f_in:
                    with open(temp_path, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
                self._extract_file(temp_path, output_path, airline, tail_number, flight_number, timestamp, level + 1)
                os.remove(temp_path)

        except PermissionError as e:
            self.logger.error(f"PermissionError: {e}")
            self.fail_count += 1
            self.errors.append(f"PermissionError: {e}")
        except Exception as e:
            self.logger.error(f"Error extracting {file_path}: {e}")
            self.fail_count += 1
            self.errors.append(f"Error extracting {file_path}: {e}")

    def _make_unique(self, path):
        """
        Ensure the file name is unique by checking if the file already exists and appending a number if necessary.
        """
        base, ext = os.path.splitext(path)
        counter = 1
        new_path = path
        while os.path.exists(new_path):
            new_path = f"{base}_{counter}{ext}"
            counter += 1
        return new_path

    def _extract_file_info(self, file_name):
        """
        Extract the airline code, tail number, flight number, and timestamp from the filename.
        Example filename: SECURELOGS_20240412005127_AAL_N882BL_AAL905.tgz
        """
        parts = file_name.split('_')
        if len(parts) > 4:
            timestamp = parts[1]
            airline_code = parts[2]
            tail_number = parts[3]
            flight_number = parts[4].split('.')[0]
            return airline_code, tail_number, flight_number, timestamp
        return 'UNKNOWN', 'UNKNOWN', 'UNKNOWN', 'UNKNOWN'

    def _log_processed_archive(self, archive_name):
        """
        Log the name of the processed archive.
        """
        log_file_name = "extraction_log.txt"
        log_file_path = os.path.join(self.logs_dir, log_file_name)

        with open(log_file_path, 'a') as log_file:
            log_file.write(f"Processed archive: {archive_name}\n")

    def _write_log_summary(self):
        """
        Write a summary log file with the extraction results.
        """
        if self.success_count == 0 and self.fail_count == 0:
            return  # Do not log if no files were processed

        log_file_name = "extraction_log.txt"
        log_file_path = os.path.join(self.logs_dir, log_file_name)

        with open(log_file_path, 'a') as log_file:
            log_file.write(f"Extraction Summary - {datetime.now()}\n")
            log_file.write(f"Total files successfully extracted: {self.success_count}\n")
            log_file.write(f"Total files failed to extract: {self.fail_count}\n")
            if self.errors:
                log_file.write("Errors encountered during extraction:\n")
                for error in self.errors:
                    log_file.write(f"- {error}\n")
            log_file.write("\n----\n\n")

        self.logger.info(f"Wrote log summary to {log_file_path}")

if __name__ == "__main__":
    raw_dir = 'raw'
    logs_dir = 'logs'
    
    # Setup logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('LogExtractor')
    
    logger.info("Starting the log extraction process...")
    
    # Create an instance of LogExtractor and start extraction
    extractor = LogExtractor(raw_dir, logs_dir, logger)
    extractor.extract()
    
    logger.info("Log extraction process completed.")