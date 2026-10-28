// ticker_server.cpp — zero-dependency TCP pub/sub quote board server (C++17)
//
// Why C++ here: the scraper (Python/tedata) produces a new snapshot every
// few minutes; distributing quote updates to many dashboard clients with
// microsecond fan-out and instant catch-up is pure I/O — exactly what a
// small C++ server does best. Zero deps: g++ -O3 -std=c++17 -pthread.
//
// Line protocol (newline-terminated, fields space-separated):
//   publisher -> server : PUB <slug> <group> <units> <price> <chg> <pct> <ts>
//   server -> all       : QUOTE <slug> <price> <chg> <pct> <ts>
//   on connect          : SNAPSHOT <slug> <price> <chg> <pct> <ts>  (one per
//                         known commodity, then ENDSNAPSHOT)
// Clients are read-only; publishers are trusted (bind localhost or firewall).

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

namespace {

volatile sig_atomic_t g_stop = 0;
void on_signal(int) { g_stop = 1; }

struct Quote {
    std::string slug, group, units, price, chg, pct, ts;
};

std::map<std::string, Quote> g_quotes;   // latest quote per commodity
std::mutex g_mu;
std::set<int> g_clients;                 // connected dashboard clients
std::mutex g_cmu;

void broadcast(const std::string& line) {
    std::lock_guard<std::mutex> lk(g_cmu);
    for (int fd : g_clients)
        if (::send(fd, line.data(), line.size(), MSG_NOSIGNAL) < 0) { /* drop */ }
}

std::vector<std::string> split(const std::string& s) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == ' ') { if (!cur.empty()) { out.push_back(cur); cur.clear(); } }
        else cur.push_back(c);
    }
    if (!cur.empty()) out.push_back(cur);
    return out;
}

void handle_conn(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));  // low latency
    { std::lock_guard<std::mutex> lk(g_cmu); g_clients.insert(fd); }

    {   // catch-up: full snapshot so late-joining boards render immediately
        std::lock_guard<std::mutex> lk(g_mu);
        for (const auto& [k, q] : g_quotes) {
            std::string line = "SNAPSHOT " + q.slug + " " + q.price + " " +
                               q.chg + " " + q.pct + " " + q.ts + "\n";
            if (::send(fd, line.data(), line.size(), MSG_NOSIGNAL) < 0) break;
        }
    }
    static const char end_snap[] = "ENDSNAPSHOT\n";
    ::send(fd, end_snap, sizeof(end_snap) - 1, MSG_NOSIGNAL);

    std::string buf;
    char tmp[4096];
    while (!g_stop) {
        ssize_t n = ::recv(fd, tmp, sizeof(tmp), 0);
        if (n <= 0) break;
        buf.append(tmp, static_cast<size_t>(n));
        size_t pos;
        while ((pos = buf.find('\n')) != std::string::npos) {
            std::string line = buf.substr(0, pos);
            buf.erase(0, pos + 1);
            if (line.rfind("PUB ", 0) != 0) continue;
            std::vector<std::string> f = split(line);
            if (f.size() < 8) continue;
            Quote q{f[1], f[2], f[3], f[4], f[5], f[6], f[7]};
            { std::lock_guard<std::mutex> lk(g_mu); g_quotes[f[1]] = q; }
            broadcast("QUOTE " + q.slug + " " + q.price + " " + q.chg + " " +
                      q.pct + " " + q.ts + "\n");
        }
    }
    { std::lock_guard<std::mutex> lk(g_cmu); g_clients.erase(fd); }
    ::close(fd);
}

}  // namespace

int main(int argc, char** argv) {
    int port = argc > 1 ? std::atoi(argv[1]) : 7379;

    struct sigaction sa {};
    sa.sa_handler = on_signal;
    sigaction(SIGINT, &sa, nullptr);            // no SA_RESTART: accept() must
    sigaction(SIGTERM, &sa, nullptr);           // wake so shutdown is clean
    ::signal(SIGPIPE, SIG_IGN);

    int srv = ::socket(AF_INET, SOCK_STREAM, 0);
    if (srv < 0) { perror("socket"); return 1; }
    int one = 1;
    setsockopt(srv, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(static_cast<uint16_t>(port));
    if (::bind(srv, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) ||
        ::listen(srv, 128)) { perror("bind/listen"); return 1; }

    std::printf("ticker_server listening on :%d\n", port);
    std::fflush(stdout);

    while (!g_stop) {
        sockaddr_in cli {};
        socklen_t len = sizeof(cli);
        int fd = ::accept(srv, reinterpret_cast<sockaddr*>(&cli), &len);
        if (fd < 0) { if (errno == EINTR) continue; if (g_stop) break; continue; }
        std::thread(handle_conn, fd).detach();
    }
    ::close(srv);
    std::printf("ticker_server stopped.\n");
    return 0;
}
