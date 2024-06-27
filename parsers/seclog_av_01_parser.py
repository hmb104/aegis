import re
import logging
from datetime import datetime

def parse_seclog_av_01_log(line):
    try:
        log_match = re.match(r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{2}\+\d{2}:\d{2}) - (\S+) - (\S+) - (\S+) - (\S+) - (\[INFO|ERROR\]) - (.*)', line)
        if log_match:
            timestamp_str = log_match.group(1)
            timestamp = datetime.fromisoformat(timestamp_str).isoformat()
            event = {
                'timestamp': timestamp,
                'source': log_match.group(2),
                'process_info': log_match.group(3),
                'user': log_match.group(4),
                'event_code': log_match.group(5),
                'log_level': log_match.group(6),
                'message': log_match.group(7)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing SecLog_AV_01 log line: {e}")

    return None