import re
import logging
from datetime import datetime

def parse_secure_log(line):
    try:
        log_match = re.match(r'(\w{3} \d{2} \d{2}:\d{2}:\d{2}) (\S+) (\S+)\[(\d+)\]: (.*)', line)
        if log_match:
            timestamp_str = log_match.group(1)
            current_year = datetime.now().year
            timestamp = datetime.strptime(f"{current_year} {timestamp_str}", '%Y %b %d %H:%M:%S').isoformat()
            event = {
                'timestamp': timestamp,
                'hostname': log_match.group(2),
                'process': log_match.group(3),
                'pid': log_match.group(4),
                'message': log_match.group(5)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing secure log line: {e}")

    return None