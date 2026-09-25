// Standalone OUCH test client: connects to a running NetworkServiceApp's
// OUCH TCP listener and sends one or more ENTER_ORDER frames. Deliberately
// decoupled from NetworkService/MatchingService internal types (it's meant
// to simulate an external client, not reuse OuchOrderCommand) — the frame
// layout is duplicated here from OuchProtocolHandler.cpp's assumed wire
// format (see NetworkService/OuchProtocolHandler.cpp), the same layout
// Testing.cpp's BuildOuchEnterOrderFrame helper builds for the ingress
// pipeline tests.
#include "OrderTypes.hpp"

#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

// [2B BE payloadLen][1B 'O'][14B token][1B side][1B orderType]
// [4B BE shares][8B symbol][4B BE price][4B BE firmId]
std::vector<char> BuildOuchEnterOrderFrame(const char orderToken[14], char buySellIndicator,
    uint32_t shares, const char symbol[8], uint32_t price, ORDER_TYPE orderType, uint32_t firmId) {
    constexpr size_t bodyLen = 14 + 1 + 1 + 4 + 8 + 4 + 4;
    constexpr uint16_t payloadLen = static_cast<uint16_t>(1 + bodyLen);

    std::vector<char> frame(2 + payloadLen);
    size_t off = 0;
    frame[off++] = static_cast<char>((payloadLen >> 8) & 0xFF);
    frame[off++] = static_cast<char>(payloadLen & 0xFF);
    frame[off++] = 'O';
    std::memcpy(frame.data() + off, orderToken, 14); off += 14;
    frame[off++] = buySellIndicator;
    frame[off++] = static_cast<char>(orderType);
    frame[off++] = static_cast<char>((shares >> 24) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 16) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 8) & 0xFF);
    frame[off++] = static_cast<char>(shares & 0xFF);
    std::memcpy(frame.data() + off, symbol, 8); off += 8;
    frame[off++] = static_cast<char>((price >> 24) & 0xFF);
    frame[off++] = static_cast<char>((price >> 16) & 0xFF);
    frame[off++] = static_cast<char>((price >> 8) & 0xFF);
    frame[off++] = static_cast<char>(price & 0xFF);
    frame[off++] = static_cast<char>((firmId >> 24) & 0xFF);
    frame[off++] = static_cast<char>((firmId >> 16) & 0xFF);
    frame[off++] = static_cast<char>((firmId >> 8) & 0xFF);
    frame[off++] = static_cast<char>(firmId & 0xFF);
    return frame;
}

// [2B BE payloadLen]['U'][14B existingToken][14B newToken][4B BE shares][4B BE price]
// Matches OuchProtocolHandler.cpp's REPLACE_ORDER case: two tokens (the
// order being replaced, and the new one identifying the result), not one
// token reused — a token identifies one specific order instance.
std::vector<char> BuildOuchReplaceOrderFrame(const char existingToken[14], const char newToken[14],
    uint32_t shares, uint32_t price) {
    constexpr size_t bodyLen = 14 + 14 + 4 + 4;
    constexpr uint16_t payloadLen = static_cast<uint16_t>(1 + bodyLen);

    std::vector<char> frame(2 + payloadLen);
    size_t off = 0;
    frame[off++] = static_cast<char>((payloadLen >> 8) & 0xFF);
    frame[off++] = static_cast<char>(payloadLen & 0xFF);
    frame[off++] = 'U';
    std::memcpy(frame.data() + off, existingToken, 14); off += 14;
    std::memcpy(frame.data() + off, newToken, 14); off += 14;
    frame[off++] = static_cast<char>((shares >> 24) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 16) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 8) & 0xFF);
    frame[off++] = static_cast<char>(shares & 0xFF);
    frame[off++] = static_cast<char>((price >> 24) & 0xFF);
    frame[off++] = static_cast<char>((price >> 16) & 0xFF);
    frame[off++] = static_cast<char>((price >> 8) & 0xFF);
    frame[off++] = static_cast<char>(price & 0xFF);
    return frame;
}

// [2B BE payloadLen]['X'][14B token][4B BE shares]
std::vector<char> BuildOuchCancelOrderFrame(const char orderToken[14], uint32_t shares) {
    constexpr size_t bodyLen = 14 + 4;
    constexpr uint16_t payloadLen = static_cast<uint16_t>(1 + bodyLen);

    std::vector<char> frame(2 + payloadLen);
    size_t off = 0;
    frame[off++] = static_cast<char>((payloadLen >> 8) & 0xFF);
    frame[off++] = static_cast<char>(payloadLen & 0xFF);
    frame[off++] = 'X';
    std::memcpy(frame.data() + off, orderToken, 14); off += 14;
    frame[off++] = static_cast<char>((shares >> 24) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 16) & 0xFF);
    frame[off++] = static_cast<char>((shares >> 8) & 0xFF);
    frame[off++] = static_cast<char>(shares & 0xFF);
    return frame;
}

void PadSymbol(char out[8], const std::string& symbol) {
    std::memset(out, ' ', 8);
    std::memcpy(out, symbol.data(), std::min<size_t>(symbol.size(), 8));
}

// 14-byte order token, unique per call within a process run.
void MakeToken(char out[14], uint32_t counter) {
    std::memset(out, ' ', 14);
    std::string text = "T" + std::to_string(counter);
    std::memcpy(out, text.data(), std::min<size_t>(text.size(), 14));
}

uint16_t ReadBE16(const char* p) {
    return (static_cast<uint8_t>(p[0]) << 8) | static_cast<uint8_t>(p[1]);
}
uint32_t ReadBE32(const char* p) {
    return (static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 24)
        | (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 16)
        | (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 8)
        | static_cast<uint32_t>(static_cast<uint8_t>(p[3]));
}
uint64_t ReadBE64(const char* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = (v << 8) | static_cast<uint8_t>(p[i]);
    return v;
}

// Decodes the egress frame NetworkService/OrderEventFrame.hpp encodes:
// [2B BE len][1B type][8B BE order_id][1B side][1B price]
// [4B BE quantity][4B BE leaves_quantity][8B BE match_id][1B reject_reason]
// Duplicated here rather than shared, matching this tool's "deliberately
// decoupled from NetworkService" philosophy (see file header comment).
void PrintOuchResponse(const char* body, size_t len) {
    if (len != 8 + 1 + 1 + 4 + 4 + 8 + 1) {
        std::cout << "  (unrecognized response body length " << len << ")\n";
        return;
    }
    size_t off = 0;
    uint64_t orderId = ReadBE64(body + off); off += 8;
    char side = body[off++];
    uint8_t price = static_cast<uint8_t>(body[off++]);
    uint32_t qty = ReadBE32(body + off); off += 4;
    uint32_t leaves = ReadBE32(body + off); off += 4;
    uint64_t matchId = ReadBE64(body + off); off += 8;
    uint8_t rejectReason = static_cast<uint8_t>(body[off++]);

    std::cout << "  order_id=" << orderId << " side=" << side
              << " price=" << static_cast<int>(price) << " qty=" << qty
              << " leaves=" << leaves << " match_id=" << matchId
              << " reject_reason=" << static_cast<int>(rejectReason) << "\n";
}

// Reads and decodes any OUCH-ack frames that arrive on `sock` within the
// next `listenMs` milliseconds. Best-effort: prints raw bytes if a frame
// doesn't parse cleanly rather than failing.
void ListenForResponses(int sock, int listenMs) {
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(listenMs);
    std::vector<char> buf;
    char chunk[512];

    while (std::chrono::steady_clock::now() < deadline) {
        auto remaining = std::chrono::duration_cast<std::chrono::microseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) break;

        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(sock, &readfds);
        timeval tv{};
        tv.tv_sec = static_cast<time_t>(remaining.count() / 1'000'000);
        tv.tv_usec = static_cast<suseconds_t>(remaining.count() % 1'000'000);

        int ready = select(sock + 1, &readfds, nullptr, nullptr, &tv);
        if (ready <= 0) continue;

        ssize_t n = recv(sock, chunk, sizeof(chunk), 0);
        if (n <= 0) break; // server closed the connection
        buf.insert(buf.end(), chunk, chunk + n);

        while (buf.size() >= 2) {
            uint16_t payloadLen = ReadBE16(buf.data());
            size_t totalFrameLen = 2 + payloadLen;
            if (buf.size() < totalFrameLen) break;

            char type = buf[2];
            std::cout << "Received OUCH response: type=" << type << "\n";
            PrintOuchResponse(buf.data() + 3, payloadLen - 1);

            buf.erase(buf.begin(), buf.begin() + static_cast<long>(totalFrameLen));
        }
    }
}

ORDER_TYPE ParseOrderType(const std::string& s) {
    if (s == "LIMIT") return ORDER_TYPE::LIMIT;
    if (s == "IOC" || s == "IMMEDIATE_OR_CANCEL") return ORDER_TYPE::IMMEDIATE_OR_CANCEL;
    if (s == "FOK" || s == "FILL_OR_KILL") return ORDER_TYPE::FILL_OR_KILL;
    if (s == "GTC" || s == "GOOD_TILL_CANCEL") return ORDER_TYPE::GOOD_TILL_CANCEL;
    if (s == "GTD" || s == "GOOD_TILL_DAY") return ORDER_TYPE::GOOD_TILL_DAY;
    if (s == "MARKET") return ORDER_TYPE::MARKET;
    throw std::runtime_error("Unknown --order-type: " + s);
}

void PrintUsage() {
    std::cerr <<
        "Usage: OuchTestClient --side B|S --price N --qty N [options]\n"
        "  --host <ip>            default 127.0.0.1\n"
        "  --port <port>          default 10001 (NetworkConfig::ouchListenPort)\n"
        "  --symbol <name>        default TEST, space-padded/truncated to 8 bytes\n"
        "  --side B|S             required for a new order (not for --replace-token)\n"
        "  --price <0-255>        required (Price is uint8_t on the exchange side)\n"
        "  --qty <shares>         required\n"
        "  --order-type <type>    LIMIT (default) | IOC | FOK | GTC | GTD | MARKET\n"
        "  --firm-id <n>          default 0. Self-trade prevention rejects two\n"
        "                         orders sharing a firm id that would otherwise\n"
        "                         cross each other — use different --firm-id\n"
        "                         values across invocations to let them match.\n"
        "  --token <text>         default: auto-generated, unique per order sent.\n"
        "                         With --replace-token, this is the NEW token.\n"
        "  --replace-token <text> send a REPLACE_ORDER instead of ENTER_ORDER,\n"
        "                         replacing the order currently identified by\n"
        "                         this existing token with one at --price/--qty\n"
        "                         (identified afterward by --token, or an\n"
        "                         auto-generated one if --token isn't given).\n"
        "                         Ignores --side/--order-type/--firm-id/--count,\n"
        "                         which don't apply to a replace.\n"
        "  --cancel-token <text>  send a CANCEL_ORDER instead of ENTER_ORDER,\n"
        "                         canceling the order currently identified by\n"
        "                         this token. Ignores --side/--price/--order-type/\n"
        "                         --firm-id/--count/--replace-token.\n"
        "  --count <n>            send n orders over one connection, default 1\n"
        "  --interval-ms <ms>     delay between sends when --count > 1, default 0\n"
        "  --listen-ms <ms>       after sending, keep the connection open and\n"
        "                         print any OUCH ack frames received for this\n"
        "                         long (default 0 = close immediately)\n";
}

} // namespace

int main(int argc, char** argv) {
    std::string host = "127.0.0.1";
    uint16_t port = 10001;
    std::string symbolStr = "TEST";
    char side = 0;
    long price = -1;
    long qty = -1;
    ORDER_TYPE orderType = ORDER_TYPE::LIMIT;
    uint32_t firmId = 0;
    std::string tokenOverride;
    std::string replaceToken;
    std::string cancelToken;
    int count = 1;
    int intervalMs = 0;
    int listenMs = 0;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) throw std::runtime_error("Missing value for " + arg);
            return argv[++i];
        };

        if (arg == "--host") host = next();
        else if (arg == "--port") port = static_cast<uint16_t>(std::stoi(next()));
        else if (arg == "--symbol") symbolStr = next();
        else if (arg == "--side") side = next()[0];
        else if (arg == "--price") price = std::stol(next());
        else if (arg == "--qty") qty = std::stol(next());
        else if (arg == "--order-type") orderType = ParseOrderType(next());
        else if (arg == "--firm-id") firmId = static_cast<uint32_t>(std::stoul(next()));
        else if (arg == "--token") tokenOverride = next();
        else if (arg == "--replace-token") replaceToken = next();
        else if (arg == "--cancel-token") cancelToken = next();
        else if (arg == "--count") count = std::stoi(next());
        else if (arg == "--interval-ms") intervalMs = std::stoi(next());
        else if (arg == "--listen-ms") listenMs = std::stoi(next());
        else if (arg == "--help") { PrintUsage(); return 0; }
        else {
            std::cerr << "Unknown argument: " << arg << "\n";
            PrintUsage();
            return 1;
        }
    }

    bool isReplace = !replaceToken.empty();
    bool isCancel = !cancelToken.empty();
    if (isReplace && isCancel) {
        std::cerr << "--replace-token and --cancel-token are mutually exclusive.\n";
        return 1;
    }
    bool needsPriceAndSide = !isReplace && !isCancel;
    if ((needsPriceAndSide && side != 'B' && side != 'S') || (!isCancel && (price < 0 || price > 255))
        || (!isCancel && qty <= 0)) {
        std::cerr << "Missing/invalid required arguments.\n";
        PrintUsage();
        return 1;
    }
    if (isReplace && replaceToken.size() > 14) {
        std::cerr << "--replace-token must be at most 14 bytes.\n";
        return 1;
    }
    if (isCancel && cancelToken.size() > 14) {
        std::cerr << "--cancel-token must be at most 14 bytes.\n";
        return 1;
    }

    char symbol[8];
    PadSymbol(symbol, symbolStr);

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) throw std::runtime_error("Unable to create socket");

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);
    if (inet_pton(AF_INET, host.c_str(), &serverAddr.sin_addr) != 1) {
        std::cerr << "Invalid --host: " << host << "\n";
        return 1;
    }
    if (connect(sock, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        std::cerr << "Unable to connect to " << host << ":" << port << "\n";
        close(sock);
        return 1;
    }

    if (isReplace) {
        char existingTokenBuf[14];
        std::memset(existingTokenBuf, ' ', 14);
        std::memcpy(existingTokenBuf, replaceToken.data(), replaceToken.size());

        char newToken[14];
        if (!tokenOverride.empty()) {
            std::memset(newToken, ' ', 14);
            std::memcpy(newToken, tokenOverride.data(), std::min<size_t>(tokenOverride.size(), 14));
        } else {
            MakeToken(newToken, static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
        }

        auto frame = BuildOuchReplaceOrderFrame(existingTokenBuf, newToken,
            static_cast<uint32_t>(qty), static_cast<uint32_t>(price));

        ssize_t sent = send(sock, frame.data(), frame.size(), 0);
        if (sent != static_cast<ssize_t>(frame.size())) {
            std::cerr << "Failed to send REPLACE_ORDER frame\n";
            close(sock);
            return 1;
        }

        std::cout << "Sent REPLACE_ORDER: existing_token=" << replaceToken
                  << " price=" << price << " qty=" << qty << "\n";
    } else if (isCancel) {
        char cancelTokenBuf[14];
        std::memset(cancelTokenBuf, ' ', 14);
        std::memcpy(cancelTokenBuf, cancelToken.data(), cancelToken.size());

        auto frame = BuildOuchCancelOrderFrame(cancelTokenBuf, qty > 0 ? static_cast<uint32_t>(qty) : 0);

        ssize_t sent = send(sock, frame.data(), frame.size(), 0);
        if (sent != static_cast<ssize_t>(frame.size())) {
            std::cerr << "Failed to send CANCEL_ORDER frame\n";
            close(sock);
            return 1;
        }

        std::cout << "Sent CANCEL_ORDER: token=" << cancelToken << "\n";
    } else {
        for (int i = 0; i < count; ++i) {
            char token[14];
            if (!tokenOverride.empty() && count == 1) {
                std::memset(token, ' ', 14);
                std::memcpy(token, tokenOverride.data(), std::min<size_t>(tokenOverride.size(), 14));
            } else {
                MakeToken(token, static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count() ^ i));
            }

            auto frame = BuildOuchEnterOrderFrame(token, side, static_cast<uint32_t>(qty), symbol,
                static_cast<uint32_t>(price), orderType, firmId);

            ssize_t sent = send(sock, frame.data(), frame.size(), 0);
            if (sent != static_cast<ssize_t>(frame.size())) {
                std::cerr << "Failed to send frame " << i << "\n";
                close(sock);
                return 1;
            }

            std::cout << "Sent ENTER_ORDER: side=" << side << " price=" << price
                      << " qty=" << qty << " symbol=" << symbolStr << " firm_id=" << firmId << "\n";

            if (i + 1 < count && intervalMs > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(intervalMs));
            }
        }
    }

    if (listenMs > 0) {
        std::cout << "Listening for responses for " << listenMs << "ms...\n";
        ListenForResponses(sock, listenMs);
    }

    close(sock);
    return 0;
}
