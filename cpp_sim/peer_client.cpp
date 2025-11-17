// peer_client_win.cpp - Windows version (Fixed inet_pton)
#include <iostream>
#include <fstream>
#include <winsock2.h>
#include <ws2tcpip.h>

// Link thư viện cho Visual Studio (nếu dùng), còn MinGW thì dùng lệnh -lws2_32
#pragma comment(lib, "ws2_32.lib")

using namespace std;
const int BUFFER = 4096;

void sendFile(const string& ip, int port, const string& filename) {
    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        cerr << "Cannot create socket" << endl;
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    // --- SỬA LỖI TẠI ĐÂY ---
    // Thay vì dùng inet_pton, ta dùng inet_addr (tương thích tốt hơn trên MinGW cũ)
    addr.sin_addr.s_addr = inet_addr(ip.c_str()); 
    if (addr.sin_addr.s_addr == INADDR_NONE) {
        cerr << "Invalid IP address" << endl;
        closesocket(sock);
        return;
    }
    // -----------------------

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        cerr << "Cannot connect to server. Is it running?" << endl;
        closesocket(sock);
        return;
    }

    ifstream ifs(filename, ios::binary);
    if (!ifs) { 
        cerr << "Cannot open input file: " << filename << endl; 
        closesocket(sock); 
        return; 
    }

    string cmd = "PUT " + filename + "\n";
    send(sock, cmd.c_str(), cmd.length(), 0);

    char buf[BUFFER];
    while (ifs.read(buf, BUFFER) || ifs.gcount() > 0) {
        send(sock, buf, ifs.gcount(), 0);
    }
    ifs.close();
    
    // Đợi phản hồi OK từ server
    char resp[128];
    int n = recv(sock, resp, sizeof(resp)-1, 0);
    if(n > 0) {
        resp[n] = 0;
        cout << "Server response: " << resp;
    }
    
    closesocket(sock);
}

void getFile(const string& ip, int port, const string& filename) {
    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) return;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    // --- SỬA LỖI TẠI ĐÂY ---
    addr.sin_addr.s_addr = inet_addr(ip.c_str());
    // -----------------------

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        cerr << "Cannot connect to server" << endl;
        return;
    }

    string cmd = "GET " + filename + "\n";
    send(sock, cmd.c_str(), cmd.length(), 0);

    ofstream ofs(string("downloaded_") + filename, ios::binary);
    char buf[BUFFER];
    int n;
    while ((n = recv(sock, buf, BUFFER, 0)) > 0) {
        ofs.write(buf, n);
    }
    ofs.close();
    closesocket(sock);
    cout << "File downloaded as downloaded_" << filename << endl;
}

int main(int argc, char** argv) {
    // Khởi tạo Winsock
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        cout << "WSAStartup failed" << endl;
        return 1;
    }

    if (argc < 4) {
        cout << "Usage: client.exe <ip> PUT/GET <filename>" << endl;
        return 0;
    }
    string ip = argv[1];
    string cmd = argv[2];
    string fn = argv[3];
    int port = 8080;

    if (cmd == "PUT") sendFile(ip, port, fn);
    else if (cmd == "GET") getFile(ip, port, fn);
    else cout << "Unknown command" << endl;

    WSACleanup();
    return 0;
}