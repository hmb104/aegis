import os
import json
import logging
import parsers
from datetime import datetime, timezone

class LogTransformer:
    def __init__(self, logs_dir, clean_dir, logger):
        """
        Initialize the LogTransformer with directories and logger.
        """
        self.logs_dir = logs_dir
        self.clean_dir = clean_dir
        self.logger = logger

    def transform(self):
        """
        Transform all log files in the logs directory and save them in the clean directory.
        """
        self.logger.debug(f"Starting transformation from logs directory: {self.logs_dir}")
        if not os.path.exists(self.logs_dir):
            self.logger.error(f"Logs directory does not exist: {self.logs_dir}")
            return
        if not os.path.exists(self.clean_dir):
            os.makedirs(self.clean_dir)
        
        for airline_dir in os.listdir(self.logs_dir):
            airline_path = os.path.join(self.logs_dir, airline_dir)
            self.logger.debug(f"Checking airline directory: {airline_path}")
            if os.path.isdir(airline_path):
                for root, _, files in os.walk(airline_path):
                    for file_name in files:
                        file_path = os.path.join(root, file_name)
                        if os.path.getsize(file_path) > 0:  # Ignore empty files
                            self.logger.debug(f"Found log file: {file_path}")
                            self._transform_file(file_path)
                        else:
                            self.logger.debug(f"Ignored empty file: {file_path}")
        self.logger.debug("Transformation process completed.")

    def _transform_file(self, file_path):
        """
        Transform a single log file and save the transformed data.
        """
        try:
            transformed_data = []
            with open(file_path, 'r') as file:
                for line in file:
                    parsed_line = self._parse_log_line(line, file_path)
                    if parsed_line:
                        transformed_data.append(parsed_line)

            clean_file_path = self._get_clean_file_path(file_path)
            os.makedirs(os.path.dirname(clean_file_path), exist_ok=True)
            with open(clean_file_path, 'w') as clean_file:
                for entry in transformed_data:
                    clean_file.write(json.dumps(entry) + "\n")

            self.logger.info(f"Transformed {file_path} to {clean_file_path}")

        except Exception as e:
            self.logger.error(f"Error transforming {file_path}: {e}")

    def _parse_log_line(self, line, file_path):
        """
        Parse a single log line and transform it into a structured format.
        """
        if 'alert' in file_path:
            return parsers.parse_alert_log(line)
        elif 'audit' in file_path:
            return parsers.parse_audit_log(line)
        elif 'firewall' in file_path:
            return parsers.parse_firewall_log(line)
        elif 'CLSSecurity' in file_path:
            return parsers.parse_cls_security_log(line)
        elif 'secure' in file_path:
            return parsers.parse_secure_log(line)
        elif 'SecureSwDownload' in file_path:
            return parsers.parse_secure_sw_download_log(line)
        elif 'SwCorruptionCheck' in file_path:
            return parsers.parse_sw_corruption_check_log(line)
        elif 'clientsSecurityLogs' in file_path:
            return parsers.parse_clients_security_logs(line)
        elif 'rsyslogStatus' in file_path:
            return parsers.parse_rsyslog_status_log(line)
        elif 'PEDEvents' in file_path:
            return parsers.parse_ped_events_log(line)
        elif 'CrewPEDEvents' in file_path:
            return parsers.parse_crew_ped_events_log(line)
        elif 'NFCEvents' in file_path:
            return parsers.parse_nfc_events_log(line)
        elif 'cls_tool' in file_path:
            return parsers.parse_cls_tool_log(line)
        elif 'AWSEvents' in file_path:
            return parsers.parse_aws_events_log(line)
        elif 'SecLog_DRM_01' in file_path:
            return parsers.parse_seclog_drm_01_log(line)
        elif 'SecLog_AV_01' in file_path:
            return parsers.parse_seclog_av_01_log(line)
        elif 'SecLog_CREWAUTH' in file_path:
            return parsers.parse_seclog_crewauth_log(line)
        elif 'SecLog_MSR_01' in file_path:
            return parsers.parse_seclog_msr_01_log(line)
        elif 'SecLog_WMCAUD_01' in file_path:
            return parsers.parse_seclog_wmcaud_01_log(line)
        else:
            return parsers.parse_generic_log(line)

    def _get_clean_file_path(self, original_path):
        """
        Generate the path for the clean file based on the original log file path.
        """
        relative_path = os.path.relpath(original_path, self.logs_dir)
        clean_file_name = f"{os.path.splitext(relative_path)[0]}_CLEAN.json"
        return os.path.join(self.clean_dir, clean_file_name)

# Main script to initialize logging and run the transformer
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger('LogTransformer')

    logs_directory = 'logs'
    clean_directory = 'clean'

    transformer = LogTransformer(logs_directory, clean_directory, logger)
    transformer.transform()