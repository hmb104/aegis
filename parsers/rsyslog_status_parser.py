import re
import logging
from datetime import datetime

def parse_rsyslog_status_log(line):
    try:
        log_match = re.match(r'(\S+ \d+ \d+:\d+:\d+) (\S+) rsyslogd: \[(.*)\] (.*)', line)
        if log_match:
            timestamp_str = log_match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%b %d %H:%M:%S').replace(year=datetime.now().year).isoformat()
            event = {
                'timestamp': timestamp,
                'hostname': log_match.group(2),
                'details': log_match.group(3),
                'message': log_match.group(4)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing rsyslog status log line: {e}")

    return None