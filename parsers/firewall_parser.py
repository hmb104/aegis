import re
import logging
from datetime import datetime

def parse_firewall_log(line):
    try:
        log_match = re.match(r'(\S+ \d+ \d+:\d+:\d+) (\S+) kernel: \[(\S+)\] (\S+) (\S+) (\S+)', line)
        if log_match:
            timestamp_str = log_match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%b %d %H:%M:%S').replace(year=datetime.now().year).isoformat()
            event = {
                'timestamp': timestamp,
                'hostname': log_match.group(2),
                'kernel_info': log_match.group(3),
                'firewall_rule': log_match.group(4),
                'source': log_match.group(5),
                'destination': log_match.group(6)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing firewall log line: {e}")

    return None