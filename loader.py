import os
import json
import logging
from datetime import datetime
from elasticsearch import Elasticsearch, helpers, exceptions
import urllib3

# Suppress insecure request warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class LogLoader:
    def __init__(self, clean_dir, state_file, log_file, es_host, es_index_prefix, logger):
        self.clean_dir = clean_dir
        self.state_file = state_file
        self.log_file = log_file
        self.es = Elasticsearch(
            es_host,
            basic_auth=(os.getenv('ES_USER'), os.getenv('ES_PASS')),
            verify_certs=False
        )
        self.es_index_prefix = es_index_prefix
        self.logger = logger
        self.processed_files = self._load_state()

    def _load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                return set(line.strip() for line in f)
        return set()

    def _save_state(self):
        with open(self.state_file, 'w') as f:
            for file in self.processed_files:
                f.write(f"{file}\n")

    def _check_es_connection(self):
        try:
            if not self.es.ping():
                raise exceptions.ConnectionError("Elasticsearch cluster is not reachable")
        except exceptions.ConnectionError as e:
            self.logger.error(f"Elasticsearch connection failed: {e}")
            raise

    def load_logs(self):
        self._check_es_connection()

        total_files = 0
        total_entries = 0

        for root, _, files in os.walk(self.clean_dir):
            for file_name in files:
                file_path = os.path.join(root, file_name)
                if file_path not in self.processed_files:
                    try:
                        with open(file_path, 'r') as f:
                            entries = [json.loads(line) for line in f if line.strip()]
                            if entries:
                                log_type = self._determine_log_type(file_name)
                                success, failed = self._bulk_insert(entries, log_type)
                                total_files += 1
                                total_entries += success
                                self.processed_files.add(file_path)
                                self.logger.info(f"Processed {file_path} with {success} entries. Failed: {failed}")
                    except Exception as e:
                        self.logger.error(f"Error processing {file_path}: {e}")

        self._save_state()
        self._write_log(total_files, total_entries)
        self.logger.info("Log loading process completed.")

    def _determine_log_type(self, file_name):
        # Determine the log type based on the file name
        if 'alert' in file_name:
            return 'alert'
        elif 'audit' in file_name:
            return 'audit'
        elif 'firewall' in file_name:
            return 'firewall'
        elif 'cls_tool' in file_name:
            return 'cls_tool'
        elif 'clientsSecurityLogs' in file_name:
            return 'clientsSecurityLogs'
        elif 'CLSSecurity' in file_name:
            return 'cls_security'
        elif 'AWSEvents' in file_name:
            return 'aws_events'
        elif 'SecLog_AV_01' in file_name:
            return 'seclog_av_01'
        elif 'SecureSwDownload' in file_name:
            return 'secure_sw_download'
        elif 'SwCorruptionCheck' in file_name:
            return 'sw_corruption_check'
        # Add more elif blocks as needed for other log types
        else:
            return 'generic'

    def _bulk_insert(self, entries, log_type):
        index_name = f"{self.es_index_prefix}_{log_type}"
        actions = [
            {
                "_index": index_name,
                "_source": entry
            }
            for entry in entries
        ]
        try:
            helpers.bulk(self.es, actions)
            return len(actions), 0
        except helpers.BulkIndexError as e:
            failed_docs = len(e.errors)
            self.logger.error(f"{failed_docs} document(s) failed to index.")
            for error in e.errors:
                self.logger.error(f"Failed document: {error}")
            return len(actions) - failed_docs, failed_docs

    def _write_log(self, total_files, total_entries):
        with open(self.log_file, 'a') as log:
            log.write(f"Log Loading Summary - {datetime.now()}\n")
            log.write(f"Total files processed: {total_files}\n")
            log.write(f"Total log entries inserted: {total_entries}\n")
            log.write("----\n")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger('LogLoader')

    clean_directory = 'clean'
    state_file_path = 'processed_files.txt'
    log_file_path = 'loading_log.txt'
    es_host = 'https://localhost:9200'
    es_index_prefix = 'logs'

    logger.info("Starting the log loading process...")

    try:
        loader = LogLoader(clean_directory, state_file_path, log_file_path, es_host, es_index_prefix, logger)
        loader.load_logs()
    except Exception as e:
        logger.error(f"Failed to start LogLoader: {e}")

    logger.info("Log loading process completed.")