import re
import logging
from datetime import datetime

def parse_aws_events_log(line):
    try:
        event_match = re.match(r'(\S+ \d+ \d+:\d+:\d+) - (\S+) - (\S+) - (\S+) - (\S+) - \[(\S+)\] - (.+)', line)
        if event_match:
            timestamp_str = event_match.group(1)
            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%dT%H:%M:%S.%fZ').isoformat()
            event = {
                'timestamp': timestamp,
                'source': event_match.group(2),
                'service': event_match.group(3),
                'process': event_match.group(4),
                'user': event_match.group(5),
                'event_level': event_match.group(6),
                'message': event_match.group(7)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing AWS events log line: {e}")

    return None