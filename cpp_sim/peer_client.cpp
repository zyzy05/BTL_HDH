// peer_client.cpp - Client da sua de tuong thich voi Python Peer (Port + Protocol)
#include <iostream>
#include <fstream>
#include <string>
#include <winsock2.h>
#include <ws2tcpip.h>

// Link thu vien Winsock cho MinGW
// Khi bien dich nho them co: -lws2_32
#pragma comment(lib, "ws2_32.lib")

using namespace std;
const int BUFFER = 4096;

// Ham gui file (PUT) - Protocol: PUT <filename/hash> <filesize>\n
void sendFile(const string& ip, int port, const string& filename) {
    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        cerr << "Error: Cannot create socket." << endl;
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip.c_str()); 

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        cerr << "Error: Cannot connect to " << ip << ":" << port << endl;
        closesocket(sock);
        return;
    }

    ifstream ifs(filename, ios::binary);
    if (!ifs) { 
        cerr << "Error: Cannot open local file: " << filename << endl; 
        closesocket(sock); 
        return; 
    }

    // 1. Tinh kich thuoc file de gui Header
    ifs.seekg(0, ios::end);
    long long filesize = ifs.tellg();
    ifs.seekg(0, ios::beg);

    // 2. Gui Header: PUT <hash> <size>\n
    string cmd = "PUT " + filename + " " + to_string(filesize) + "\n";
    send(sock, cmd.c_str(), cmd.length(), 0);

    // 3. Gui noi dung file
    char buf[BUFFER];
    while (ifs.read(buf, BUFFER) || ifs.gcount() > 0) {
        send(sock, buf, ifs.gcount(), 0);
    }
    ifs.close();

    // 4. Quan trong: Bao hieu da gui xong du lieu
    shutdown(sock, SD_SEND);
    
    // 5. Doi phan hoi OK tu Server
    char resp[128];
    int n = recv(sock, resp, sizeof(resp)-1, 0);
    if(n > 0) {
        resp[n] = 0;
        cout << "Server response: " << resp; // Mong doi: "OK\n"
    }
    
    closesocket(sock);
}

// Ham tai file (GET) - Protocol: GET <hash>\n -> Nhan Header SIZE <bytes>\n -> Nhan Data
void getFile(const string& ip, int port, const string& filename) {
    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) return;

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = inet_addr(ip.c_str());

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        cerr << "Error: Cannot connect to " << ip << ":" << port << endl;
        closesocket(sock);
        return;
    }

    // 1. Gui lenh GET
    string cmd = "GET " + filename + "\n";
    send(sock, cmd.c_str(), cmd.length(), 0);

    // 2. Doc Header (Tung byte mot cho den khi gap xuong dong)
    string header = "";
    char c;
    while(true) {
        int r = recv(sock, &c, 1, 0);
        if (r <= 0) break;
        header += c;
        if (c == '\n') break;
    }

    // 3. Kiem tra Header
    if (header.find("SIZE") == 0) {
        cout << "Header received: " << header; // Vi du: SIZE 12345
        
        ofstream ofs(string("downloaded_") + filename, ios::binary);
        char buf[BUFFER];
        int n;
        // Nhan du lieu den khi het
        while ((n = recv(sock, buf, BUFFER, 0)) > 0) {
            ofs.write(buf, n);
        }
        ofs.close();
        cout << "Success: File saved as 'downloaded_" << filename << "'" << endl;
    } 
    else if (header.find("ERR") == 0) {
        cout << "Server Error: " << header;
    }
    else {
        // Truong hop Server C++ cu (khong gui header SIZE), cu nhan bua
        cout << "Warning: No SIZE header. Receiving raw stream..." << endl;
        ofstream ofs(string("downloaded_") + filename, ios::binary);
        // Ghi lai nhung gi da doc trong header (vi no khong phai header)
        ofs.write(header.c_str(), header.length());
        
        char buf[BUFFER];
        int n;
        while ((n = recv(sock, buf, BUFFER, 0)) > 0) {
            ofs.write(buf, n);
        }
        ofs.close();
        cout << "File saved." << endl;
    }

    closesocket(sock);
}

int main(int argc, char** argv) {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        cout << "WSAStartup failed" << endl;
        return 1;
    }

    // Cap nhat: Yeu cau nhap PORT
    if (argc < 5) {
        cout << "Usage: peer_client.exe <ip> <port> PUT/GET <filename_or_hash>" << endl;
        cout << "Ex: peer_client.exe 127.0.0.1 9011 GET 7e6313..." << endl;
        return 0;
    }

    string ip = argv[1];
    int port = atoi(argv[2]); // Doc port tu tham so thu 2
    string cmd = argv[3];
    string fn = argv[4];

    if (cmd == "PUT") sendFile(ip, port, fn);
    else if (cmd == "GET") getFile(ip, port, fn);
    else cout << "Unknown command: " << cmd << endl;

    WSACleanup();
    return 0;
}