import re
import logging
from datetime import datetime

def parse_clients_security_logs(line):
    try:
        log_match = re.match(r'(\S+ \d+ \d+:\d+:\d+) (\S+) sshd\[(\d+)\]: (.+)', line)
        if log_match:
            timestamp_str = log_match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%b %d %H:%M:%S').replace(year=datetime.now().year).isoformat()
            event = {
                'timestamp': timestamp,
                'source': log_match.group(2),
                'process_id': log_match.group(3),
                'message': log_match.group(4)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing clients security logs line: {e}")

    return None