import re
import logging
from datetime import datetime

def parse_audit_log(line):
    try:
        avc_match = re.search(r'type=AVC msg=audit\((\d+\.\d+):\d+\): avc: denied {(.*?)} for pid=(\d+) comm="(.*?)" path="(.*?)" dev=(.*?) ino=(\d+) scontext=(.*?) tcontext=(.*?) tclass=(.*?)', line)
        syscall_match = re.search(r'type=SYSCALL msg=audit\((\d+\.\d+):\d+\): arch=(.*?) syscall=(\d+) success=(.*?) exit=(\d+) a0=(.*?) a1=(.*?) a2=(.*?) a3=(.*?) items=(\d+) ppid=(\d+) pid=(\d+) auid=(\d+) uid=(\d+) gid=(\d+) euid=(\d+) suid=(\d+) fsuid=(\d+) egid=(\d+) sgid=(\d+) fsgid=(\d+) tty=(.*?) ses=(\d+) comm="(.*?)" exe="(.*?)" subj=(.*?) key=(.*?)', line)

        if avc_match:
            timestamp = datetime.utcfromtimestamp(float(avc_match.group(1))).isoformat()
            event = {
                'type': 'AVC',
                'timestamp': timestamp,
                'denied_action': avc_match.group(2),
                'pid': avc_match.group(3),
                'command': avc_match.group(4),
                'path': avc_match.group(5),
                'device': avc_match.group(6),
                'inode': avc_match.group(7),
                'source_context': avc_match.group(8),
                'target_context': avc_match.group(9),
                'target_class': avc_match.group(10)
            }
            return event

        if syscall_match:
            timestamp = datetime.utcfromtimestamp(float(syscall_match.group(1))).isoformat()
            event = {
                'type': 'SYSCALL',
                'timestamp': timestamp,
                'architecture': syscall_match.group(2),
                'syscall': syscall_match.group(3),
                'success': syscall_match.group(4),
                'exit_code': syscall_match.group(5),
                'arguments': {
                    'a0': syscall_match.group(6),
                    'a1': syscall_match.group(7),
                    'a2': syscall_match.group(8),
                    'a3': syscall_match.group(9)
                },
                'items': syscall_match.group(10),
                'ppid': syscall_match.group(11),
                'pid': syscall_match.group(12),
                'auid': syscall_match.group(13),
                'uid': syscall_match.group(14),
                'gid': syscall_match.group(15),
                'euid': syscall_match.group(16),
                'suid': syscall_match.group(17),
                'fsuid': syscall_match.group(18),
                'egid': syscall_match.group(19),
                'sgid': syscall_match.group(20),
                'fsgid': syscall_match.group(21),
                'tty': syscall_match.group(22),
                'session_id': syscall_match.group(23),
                'command': syscall_match.group(24),
                'executable': syscall_match.group(25),
                'subject': syscall_match.group(26),
                'key': syscall_match.group(27)
            }
            return event
    except Exception as e:
        logging.error(f"Error parsing audit log line: {e}")

    return None