import logging

def parse_crew_ped_events_log(line):
    try:
        parts = line.split(' ')
        date = parts[0] + ' ' + parts[1]
        time = parts[2]
        host = parts[3]
        process = parts[4].strip(':')
        message = ' '.join(parts[5:])
        return {
            'date': date,
            'time': time,
            'host': host,
            'process': process,
            'message': message
        }
    except (ValueError, IndexError) as e:
        logging.error(f"Error parsing crew PED events log line: {e}")
        return None
