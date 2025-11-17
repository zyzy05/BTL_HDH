// peer_server.cpp - Phiên bản chạy trên Windows (Winsock)
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstring>
#include <winsock2.h> // Thư viện Socket của Windows
#include <ws2tcpip.h>
#include <direct.h>   // Thư viện để tạo thư mục (_mkdir)

// Link thư viện ws2_32 cho MinGW (dù đã thêm cờ -lws2_32, dòng này hỗ trợ thêm cho một số IDE)
#pragma comment(lib, "ws2_32.lib")

using namespace std;

const int PORT = 8080;
const int BUFFER = 4096;

void handleClient(SOCKET clientSock) {
    char buffer[BUFFER];
    memset(buffer, 0, BUFFER);
    int n = recv(clientSock, buffer, BUFFER, 0);
    if (n <= 0) {
        closesocket(clientSock);
        return;
    }

    string header(buffer, n);
    stringstream ss(header);
    string cmd, filename;
    ss >> cmd >> filename;

    if (cmd == "PUT") {
        // Trên Windows dùng _mkdir thay vì mkdir
        _mkdir("storage"); 
        
        string filepath = "storage/" + filename;
        ofstream ofs(filepath, ios::binary);
        
        if (!ofs) {
            cout << "Error opening file for writing: " << filepath << endl;
        } else {
            // Ghi phần dữ liệu còn dư trong buffer header (sau dấu xuống dòng)
            size_t pos = header.find('\n');
            if (pos != string::npos && pos + 1 < (size_t)n) {
                ofs.write(buffer + pos + 1, n - pos - 1);
            }

            // Nhận tiếp dữ liệu còn lại
            while ((n = recv(clientSock, buffer, BUFFER, 0)) > 0) {
                ofs.write(buffer, n);
                // Trong demo đơn giản, nếu nhận ít hơn buffer thì coi như hết file
                if (n < BUFFER) break; 
            }
            ofs.close();
            string ok = "OK Received\n";
            send(clientSock, ok.c_str(), ok.length(), 0);
            cout << "Saved file: " << filename << endl;
        }

    } else if (cmd == "GET") {
        string filepath = "storage/" + filename;
        ifstream ifs(filepath, ios::binary);
        if (!ifs) {
            string err = "ERR Not found\n";
            send(clientSock, err.c_str(), err.length(), 0);
            cout << "File not found: " << filename << endl;
        } else {
            char buf[BUFFER];
            while (ifs.read(buf, BUFFER) || ifs.gcount() > 0) {
                send(clientSock, buf, ifs.gcount(), 0);
            }
            cout << "Sent file: " << filename << endl;
        }
    } else {
        string err = "ERR Unknown Command\n";
        send(clientSock, err.c_str(), err.length(), 0);
    }
    closesocket(clientSock);
}

int main() {
    // 1. Khởi tạo Winsock (Bắt buộc trên Windows)
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        cout << "WSAStartup failed" << endl;
        return 1;
    }

    // 2. Tạo Socket
    SOCKET serverSock = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSock == INVALID_SOCKET) {
        cout << "Socket creation failed" << endl;
        WSACleanup();
        return 1;
    }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    // 3. Bind
    if (bind(serverSock, (sockaddr*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        cout << "Bind failed. Error code: " << WSAGetLastError() << endl;
        closesocket(serverSock);
        WSACleanup();
        return 1;
    }

    // 4. Listen
    listen(serverSock, 5);
    cout << "Windows C++ Peer Server listening on port " << PORT << endl;

    // 5. Accept Loop
    while (true) {
        sockaddr_in clientAddr{};
        int len = sizeof(clientAddr);
        SOCKET clientSock = accept(serverSock, (sockaddr*)&clientAddr, &len);
        if (clientSock != INVALID_SOCKET) {
            // Xử lý kết nối
            handleClient(clientSock);
        }
    }

    closesocket(serverSock);
    WSACleanup();
    return 0;
}