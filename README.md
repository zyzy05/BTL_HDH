Complete P2P Project - Full Package
----------------------------------
Files included:
- tracker.py
- replication.py
- replication_utils.py
- network_socket.py
- peer.py
- test_p2p_system.py
- cpp_sim/peer_server.cpp, peer_client.cpp
- replication_log.txt (initial)

Quickstart:
1. pip install flask requests
2. Run tracker: python tracker.py
3. Run peers: in two terminals set env and run: PEER_ID=peer1 REST_PORT=9001 SOCKET_PORT=9001 python peer.py
4. Run replication: python replication.py (or replication manager starts inside tracker by default)
5. To run automated test: python test_p2p_system.py
