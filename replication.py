# replication.py
"""Replication Manager: monitor peers and ensure replication factor"""
import time, threading, requests, sys
from replication_utils import log_event, choose_source_peer, choose_replication_target

# -----------------------------
# Cấu hình
# -----------------------------
TRACKER_API = "http://127.0.0.1:5000"
# Kết nối với IP máy thứ 2
HEARTBEAT_TIMEOUT = 30
CHECK_INTERVAL = 5
REPLICATION_FACTOR = 2
TRACKER_RETRY = 5       # số lần retry khi tracker chưa sẵn sàng
TRACKER_RETRY_DELAY = 2 # giây giữa mỗi lần retry

class ReplicationManager(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.running = True

    def run(self):
        log_event("Replication Manager started.")
        while self.running:
            try:
                peers = self.get_peers()
                files = self.get_files()
                offline = self.detect_offline(peers)
                if offline:
                    log_event(f"Detected offline peers: {offline}")
                self.check_replication(files, peers)
            except Exception as e:
                log_event(f"[ERROR] replication main loop: {e}")
            time.sleep(CHECK_INTERVAL)

    # -----------------------------
    # Lấy danh sách peers từ tracker (retry nếu tracker chưa sẵn sàng)
    # -----------------------------
    def get_peers(self):
        for attempt in range(TRACKER_RETRY):
            try:
                r = requests.get(f"{TRACKER_API}/peers", timeout=5)
                return r.json()
            except requests.exceptions.RequestException as e:
                log_event(f"[WARN] get_peers failed (attempt {attempt+1}): {e}")
                time.sleep(TRACKER_RETRY_DELAY)
        raise ConnectionError(f"Cannot connect to tracker at {TRACKER_API}")

    # -----------------------------
    # Lấy danh sách files từ tracker (retry)
    # -----------------------------
    def get_files(self):
        for attempt in range(TRACKER_RETRY):
            try:
                r = requests.get(f"{TRACKER_API}/files", timeout=5)
                return r.json()
            except requests.exceptions.RequestException as e:
                log_event(f"[WARN] get_files failed (attempt {attempt+1}): {e}")
                time.sleep(TRACKER_RETRY_DELAY)
        raise ConnectionError(f"Cannot connect to tracker at {TRACKER_API}")

    # -----------------------------
    # Phát hiện peer offline
    # -----------------------------
    def detect_offline(self, peers):
        now = time.time()
        offline = []
        for pid, info in peers.items():
            try:
                last = float(info.get('last_heartbeat', 0))
            except:
                last = 0
            if info.get('status') == 'online' and now - last > HEARTBEAT_TIMEOUT:
                offline.append(pid)
                try:
                    requests.post(f"{TRACKER_API}/peer_offline", json={"peer_id": pid}, timeout=3)
                except Exception as e:
                    log_event(f"[WARN] notify peer_offline failed for {pid}: {e}")
        return offline

    # -----------------------------
    # Kiểm tra replication factor của các chunk
    # -----------------------------
    def check_replication(self, files, peers):
        for fname, meta in files.items():
            for chunk in meta.get('chunks', []):
                plist = chunk.get('peers', [])
                if len(plist) < REPLICATION_FACTOR:
                    log_event(f"Under-replicated chunk {chunk['hash']} ({len(plist)}/{REPLICATION_FACTOR})")
                    src = choose_source_peer(plist, peers)
                    dst = choose_replication_target(peers, plist)
                    if not src or not dst:
                        log_event(f"[WARN] cannot find src/dst for chunk {chunk['hash']}")
                        continue
                    self.send_replicate_task(fname, chunk['hash'], src, dst)

    # -----------------------------
    # Gửi task replicate đến peer nguồn
    # -----------------------------
    def send_replicate_task(self, file_name, chunk_hash, src_peer, dst_peer):
        task = {"task_id": f"t{int(time.time())}_{chunk_hash}",
                "src_peer": src_peer, "dst_peer": dst_peer,
                "chunk_hash": chunk_hash, "file_name": file_name}
        try:
            peers = self.get_peers()
            src_info = peers.get(src_peer)
            if not src_info:
                log_event(f"[ERROR] src peer info not found: {src_peer}")
                return
            url = f"http://{src_info['ip']}:{src_info['port']}/replicate"
            res = requests.post(url, json=task, timeout=5)
            if res.status_code == 200:
                log_event(f"Sent replicate task to {src_peer} -> {dst_peer} for {chunk_hash}")
            else:
                log_event(f"[ERROR] replicate endpoint responded {res.status_code}")
        except Exception as e:
            log_event(f"[ERROR] send_replicate_task error: {e}")

# -----------------------------
# Main: chạy replication manager
# -----------------------------
if __name__ == '__main__':
    # chờ tracker sẵn sàng trước khi start
    import time
    print("Replication Manager starting, waiting for tracker...")
    for _ in range(TRACKER_RETRY):
        try:
            import requests
            requests.get(f"{TRACKER_API}/peers", timeout=5)
            break
        except:
            time.sleep(TRACKER_RETRY_DELAY)
    mgr = ReplicationManager()
    mgr.start()
    print("Replication Manager running. Press CTRL+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping Replication Manager...")
        mgr.running = False
        sys.exit(0)
