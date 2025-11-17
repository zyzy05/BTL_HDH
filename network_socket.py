# network_socket.py - helper for socket transfers (client side and server handler provided by peer.py)
import socket, os
from replication_utils import log_event, verify_hash

def send_chunk(dst_ip, dst_port, chunk_hash, chunk_path, timeout=10):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((dst_ip, dst_port))
        size = os.path.getsize(chunk_path)
        header = f"PUT {chunk_hash} {size}\n".encode()
        s.sendall(header)
        with open(chunk_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                s.sendall(chunk)
        ack = s.recv(128).decode().strip()
        s.close()
        return ack.startswith('OK')
    except Exception as e:
        log_event(f"[ERR send_chunk] {e}")
        try:
            s.close()
        except:
            pass
        return False

def request_chunk(src_ip, src_port, chunk_hash, out_path, timeout=10):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect((src_ip, src_port))
        header = f"GET {chunk_hash}\n".encode()
        s.sendall(header)
        resp = b''
        while not resp.endswith(b'\n'):
            part = s.recv(1)
            if not part:
                break
            resp += part
        line = resp.decode().strip()
        if line.startswith('SIZE'):
            _, size_str = line.split()
            size = int(size_str)
            with open(out_path + '.tmp', 'wb') as f:
                remaining = size
                while remaining > 0:
                    data = s.recv(min(4096, remaining))
                    if not data:
                        break
                    f.write(data)
                    remaining -= len(data)
            s.close()
            if verify_hash(out_path + '.tmp', chunk_hash):
                os.replace(out_path + '.tmp', out_path)
                return True
            else:
                os.remove(out_path + '.tmp')
                return False
        else:
            s.close()
            return False
    except Exception as e:
        log_event(f"[ERR request_chunk] {e}")
        try:
            s.close()
        except:
            pass
        return False
