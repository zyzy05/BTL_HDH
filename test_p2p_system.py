# test_p2p_system.py
import subprocess, time, os, sys, requests
from pathlib import Path

base = Path(__file__).parent
CHUNK_DIR = base / "chunks"
CHUNK_DIR.mkdir(exist_ok=True)

# ---- QUAN TRỌNG: CẤU HÌNH CỔNG KHÁC NHAU ----
# REST Port (9001) != Socket Port (9011)
PEER1_ID, PEER1_REST, PEER1_SOCKET = "peer1", 9001, 9011
PEER2_ID, PEER2_REST, PEER2_SOCKET = "peer2", 9002, 9012
TRACKER_API = "http://127.0.0.1:5000"
PYTHON_EXE = sys.executable 

cmds = {
    'tracker': [PYTHON_EXE, "tracker.py"],
    # Đảm bảo truyền đúng 3 tham số vào đây
    'peer1': [PYTHON_EXE, "peer.py", PEER1_ID, str(PEER1_REST), str(PEER1_SOCKET)],
    'peer2': [PYTHON_EXE, "peer.py", PEER2_ID, str(PEER2_REST), str(PEER2_SOCKET)],
    'replication': [PYTHON_EXE, "replication.py"]
}

procs = {}

try:
    print("Starting tracker...")
    procs['tracker'] = subprocess.Popen(cmds['tracker'], cwd=base)
    time.sleep(3)

    print("Starting peer1 (REST: 9001, SOCK: 9011)...")
    procs['peer1'] = subprocess.Popen(cmds['peer1'], cwd=base)
    
    print("Starting peer2 (REST: 9002, SOCK: 9012)...")
    procs['peer2'] = subprocess.Popen(cmds['peer2'], cwd=base)
    time.sleep(3)

    print("Starting replication manager...")
    procs['replication'] = subprocess.Popen(cmds['replication'], cwd=base)
    time.sleep(2)

    print("Creating test chunk...")
    test_file = CHUNK_DIR / "demo.txt"
    with open(test_file, "w") as f:
        f.write("This is a test file for P2P replication.\n" * 50)
    
    import hashlib
    chunk_hash = hashlib.sha256(test_file.read_bytes()).hexdigest()
    chunk_path = CHUNK_DIR / f"{chunk_hash}.chunk"
    if chunk_path.exists():
        try: chunk_path.unlink()
        except: pass
    test_file.rename(chunk_path)

    print(f"Notifying Peer1 about chunk {chunk_hash[:6]}...")
    try:
        requests.post(f"http://127.0.0.1:{PEER1_REST}/replicate_done", json={"chunk_hash": chunk_hash})
    except Exception as e:
        print(f"Error contacting Peer1: {e}")

    print("Waiting for replication manager...")
    time.sleep(15)

    # Check log
    log_file = base / "replication_log.txt"
    if log_file.exists():
        print("\n--- Checking logs ---")
        with open(log_file, "r", encoding="utf-8") as f:
             print(f.read()[-500:])

finally:
    print("\nCleaning up...")
    for p in procs.values():
        try: p.terminate()
        except: pass