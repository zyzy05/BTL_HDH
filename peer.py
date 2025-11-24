# peer.py
from flask import Flask, request, jsonify
import threading, socket, os, sys, time, requests
from replication_utils import log_event, verify_hash
from network_socket import send_chunk

app = Flask(__name__)
CHUNK_DIR = 'chunks'
os.makedirs(CHUNK_DIR, exist_ok=True)

# Cấu hình mặc định
PEER_ID = 'peer1'
REST_PORT = 9001
SOCKET_PORT = 9011
TRACKER = 'http://127.0.0.1:5000'
MY_IP = '127.0.0.1'

if len(sys.argv) > 1: PEER_ID = sys.argv[1]
if len(sys.argv) > 2: REST_PORT = int(sys.argv[2])
if len(sys.argv) > 3: SOCKET_PORT = int(sys.argv[3])
if len(sys.argv) > 4: TRACKER = sys.argv[4]

def chunk_path(hashid):
    return os.path.join(CHUNK_DIR, f"{hashid}.chunk")

def register_with_tracker():
    while True:
        try:
            data = {
                'peer_id': PEER_ID,
                'ip': MY_IP,
                'port': REST_PORT,
                'socket_port': SOCKET_PORT 
            }
            requests.post(f"{TRACKER}/register", json=data, timeout=2)
            log_event(f"Registered with tracker as {PEER_ID} (REST:{REST_PORT}, SOCK:{SOCKET_PORT})")
            break
        except Exception as e:
            log_event(f"Tracker not ready, retrying... {e}")
            time.sleep(2)

def send_heartbeat():
    while True:
        time.sleep(10)
        try:
            requests.post(f"{TRACKER}/heartbeat", json={'peer_id': PEER_ID}, timeout=2)
        except:
            pass

@app.route('/replicate', methods=['POST'])
def replicate():
    task = request.get_json()
    dst = task.get('dst_peer')
    chunk_hash = task.get('chunk_hash')
    file_name = task.get('file_name')
    path = chunk_path(chunk_hash)
    
    if not os.path.exists(path):
        return jsonify({'status':'error','msg':'chunk not found'}), 404
    
    try:
        r = requests.get(f"{TRACKER}/peers")
        peers_info = r.json()
        dst_info = peers_info.get(dst)
        
        if dst_info:
            target_port = dst_info.get('socket_port', dst_info['port'])
            dst_addr = (dst_info['ip'], int(target_port))
        else:
            return jsonify({'status':'error','msg':'dst peer not found'}), 404

        ok = send_chunk(dst_addr[0], dst_addr[1], chunk_hash, path)
        if ok:
            # Thông báo thành công cho Tracker để cập nhật vị trí file mới
            requests.post(f"{TRACKER}/update_file", json={'file_name':file_name,'chunk_hash':chunk_hash,'new_peer':dst})
            return jsonify({'status':'ok'}), 200
        else:
            return jsonify({'status':'error','msg':'transfer failed'}), 500
    except Exception as e:
        log_event(f"[ERR replicate handler] {e}")
        return jsonify({'status':'error','msg':str(e)}), 500

@app.route('/replicate_done', methods=['POST'])
def replicate_done():
    data = request.get_json()
    chunk_hash = data.get('chunk_hash')
    requests.post(f"{TRACKER}/publish", json={
        'file_name': 'demo.txt',
        'chunks': [{'hash': chunk_hash, 'peers': [PEER_ID]}]
    })
    return jsonify({'status':'ack'})

def socket_server():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        s.bind(('0.0.0.0', SOCKET_PORT))
        s.listen(5)
        log_event(f"Socket server listening on {SOCKET_PORT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()
    except Exception as e:
        log_event(f"Socket bind failed: {e}")

# --- ĐÂY LÀ HÀM ĐÃ ĐƯỢC SỬA LỖI (QUAN TRỌNG) ---
def handle_conn(conn, addr):
    try:
        conn.settimeout(10)
        header = b''
        while not header.endswith(b'\n'):
            part = conn.recv(1)
            if not part: return
            header += part
        parts = header.decode().strip().split()
        if not parts: return
        
        cmd = parts[0]
        
        # --- XỬ LÝ NHẬN FILE (PUT) ---
        if cmd == 'PUT':
            if len(parts) < 3: return
            _, chash, size_str = parts
            size = int(size_str)
            tmp = os.path.join(CHUNK_DIR, chash + '.tmp')
            with open(tmp, 'wb') as f:
                remaining = size
                while remaining > 0:
                    data = conn.recv(min(4096, remaining))
                    if not data: break
                    f.write(data)
                    remaining -= len(data)
            
            if verify_hash(tmp, chash):
                final = os.path.join(CHUNK_DIR, chash + '.chunk')
                if os.path.exists(final): os.remove(final)
                os.replace(tmp, final)
                conn.sendall(b'OK\n')
                log_event(f"Received chunk {chash[:6]}... from {addr}")
            else:
                conn.sendall(b'ERR hash\n')
                if os.path.exists(tmp): os.remove(tmp)

        # --- XỬ LÝ GỬI FILE (GET) - ĐÃ BỔ SUNG ---
        elif cmd == 'GET':
            if len(parts) < 2: return
            chash = parts[1]
            path = chunk_path(chash)
            
            if os.path.exists(path):
                # Protocol yêu cầu gửi kích thước trước: "SIZE <bytes>\n"
                size = os.path.getsize(path)
                conn.sendall(f"SIZE {size}\n".encode())
                
                with open(path, 'rb') as f:
                    while True:
                        data = f.read(4096)
                        if not data: break
                        conn.sendall(data)
                log_event(f"Sent chunk {chash[:6]}... to {addr}")
            else:
                conn.sendall(b'ERR Not found\n')

    except Exception as e:
        log_event(f"[ERR handle_conn] {e}")
    finally:
        conn.close()

if __name__ == '__main__':
    threading.Thread(target=socket_server, daemon=True).start()
    threading.Thread(target=register_with_tracker, daemon=True).start()
    threading.Thread(target=send_heartbeat, daemon=True).start()
    print(f"Peer {PEER_ID} starting: REST {REST_PORT}, Socket {SOCKET_PORT}")
    app.run(host='0.0.0.0', port=REST_PORT, threaded=True)