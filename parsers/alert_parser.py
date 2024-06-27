import logging

def parse_alert_log(line):
    try:
        date_time_str, rest = line.split(" [**] ", 1)
        date_str, time_str = date_time_str.split("-")
        rule_id = rest.split("]")[0].split("[")[1]
        description = rest.split("]")[1].split("[")[0].strip()
        protocol = rest.split("{")[1].split("}")[0]
        src_ip, dst_ip = rest.split("} ")[1].split(" -> ")

        parsed_line = {
            "date": date_str,
            "time": time_str,
            "rule_id": rule_id,
            "description": description,
            "protocol": protocol,
            "source_ip": src_ip,
            "destination_ip": dst_ip
        }
        return parsed_line
    except Exception as e:
        logging.error(f"Error parsing alert log line: {e}")
        return None