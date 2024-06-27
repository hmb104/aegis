import logging

def parse_seclog_crewauth_log(line):
    try:
        parts = line.split(' ')
        date = parts[0]
        time = parts[1]
        host = parts[2]
        process = parts[3].strip(':')
        message = ' '.join(parts[4:])
        return {
            'date': date,
            'time': time,
            'host': host,
            'process': process,
            'message': message
        }
    except (ValueError, IndexError) as e:
        logging.error(f"Error parsing SecLog_CREWAUTH log line: {e}")
        return None
