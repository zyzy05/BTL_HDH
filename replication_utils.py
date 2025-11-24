# replication_utils.py
import time, random, os, hashlib

LOG_FILE = "replication_log.txt"

def log_event(msg):
    ts = time.strftime("[%Y-%m-%d %H:%M:%S]")
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(f"{ts} {msg}\n")
    print(ts, msg)

def choose_source_peer(peer_list, all_peers):
    """Select an online source peer from peer_list."""
    online = [p for p in peer_list if all_peers.get(p, {}).get("status") == "online"]
    return random.choice(online) if online else None

def choose_replication_target(all_peers, exclude):
    """Select an online destination peer not in exclude list."""
    candidates = [pid for pid,info in all_peers.items() if info.get("status") == "online" and pid not in exclude]
    return random.choice(candidates) if candidates else None

def verify_hash(path, expected_hex):
    """Return True if SHA256 of file at path equals expected_hex"""
    if not os.path.exists(path):
        return False
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            h.update(chunk)
    return h.hexdigest() == expected_hex
