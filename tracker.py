# tracker.py - Tracker server for P2P system
from flask import Flask, jsonify, request
import time

app = Flask(__name__)

# In-memory database
peers = {}
files = {}

def now():
    return time.time()

@app.route('/peers', methods=['GET'])
def get_peers():
    return jsonify(peers)

@app.route('/files', methods=['GET'])
def get_files():
    return jsonify(files)

@app.route('/register', methods=['POST'])
def register():
    data = request.json
    pid = data.get('peer_id')
    if pid:
        peers[pid] = {
            'ip': data.get('ip', '127.0.0.1'),
            'port': data.get('port'),       # REST Port (Flask)
            'socket_port': data.get('socket_port'), # Socket Port (File Transfer) - MỚI
            'last_heartbeat': now(),
            'status': 'online'
        }
        print(f"Peer registered: {pid} (REST: {data.get('port')}, Socket: {data.get('socket_port')})")
    return jsonify({'status':'ok'})

@app.route('/heartbeat', methods=['POST'])
def heartbeat():
    data = request.json
    pid = data.get('peer_id')
    if pid in peers:
        peers[pid]['last_heartbeat'] = now()
        peers[pid]['status'] = 'online'
    return jsonify({'status':'ok'})

@app.route('/publish', methods=['POST'])
def publish():
    data = request.json
    fname = data.get('file_name')
    files[fname] = data
    return jsonify({'status':'ok'})

@app.route('/peer_offline', methods=['POST'])
def peer_offline():
    data = request.json
    pid = data.get('peer_id')
    if pid in peers:
        peers[pid]['status'] = 'offline'
        print(f"Peer reported offline: {pid}")
    return jsonify({'status':'ok'})

@app.route('/update_file', methods=['POST'])
def update_file():
    data = request.json
    fname = data.get('file_name')
    chash = data.get('chunk_hash')
    new_peer = data.get('new_peer')
    if fname in files:
        for ch in files[fname].get('chunks', []):
            if ch['hash'] == chash:
                if new_peer not in ch['peers']:
                    ch['peers'].append(new_peer)
    return jsonify({'status':'ok'})

if __name__ == '__main__':
    print("Tracker started on port 5000...")
    app.run(port=5000, threaded=True)