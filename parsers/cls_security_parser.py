import re
import logging
from datetime import datetime

def parse_cls_security_log(line):
    try:
        log_match = re.match(r'(\S+ \d+ \d+:\d+:\d+) (\S+) CLS-SECURITY: \[([^\]]+)\]\[(\S+)\] : \[(\S+)\] : (.+)', line)
        if log_match:
            timestamp_str = log_match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%b %d %H:%M:%S').replace(year=datetime.now().year).isoformat()
            event = {
                'timestamp': timestamp,
                'source': log_match.group(2),
                'event_bus': log_match.group(3),
                'log_level': log_match.group(4),
                'dsu': log_match.group(5),
                'message': log_match.group(6)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing CLS security log line: {e}")

    return None