import re
import logging
from datetime import datetime

def parse_cls_tool_log(line):
    try:
        log_match = re.match(r'(\S+ \d+ \d+:\d+:\d+) (\S+) cls-tool: (.+)', line)
        if log_match:
            timestamp_str = log_match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%b %d %H:%M:%S').replace(year=datetime.now().year).isoformat()
            event = {
                'timestamp': timestamp,
                'hostname': log_match.group(2),
                'message': log_match.group(3)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing CLS tool log line: {e}")

    return None