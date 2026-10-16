#pragma once
// Types shared by the ingress benchmark and its transport rigs (see IngressBench.cpp).

#include "NetworkGateway.hpp"
#include "SPSCQueue.hpp"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

namespace bench {

using Clock = std::chrono::steady_clock;

// The stand-in command is sized so a sequenced record is 128 bytes, like the
// reference product's. That fixes how many records fit in a datagram.
struct BenchCommand {
    TraceId trace_id;
    SessionId sessionId;
    uint64_t scheduledSendNs; // set by the client, from the same clock the drain threads read
    char pad[88];
};
using BenchMessage = SequencedInboundMessage<BenchCommand>;
static_assert(sizeof(BenchMessage) == 128, "the benchmark record is meant to be 128 bytes");

constexpr size_t FRAME_BYTES = sizeof(uint64_t);

struct FrameHandler {
    static constexpr size_t PARSE_FATAL_ERROR = SIZE_MAX;
    size_t validateAndParse(const char* buf, size_t availableLen, BenchCommand& out) {
        if (availableLen < FRAME_BYTES) return 0;
        std::memcpy(&out.scheduledSendNs, buf, FRAME_BYTES);
        return FRAME_BYTES;
    }
};

inline uint64_t NowNs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

struct Options {
    std::string transport = "handwritten";
    int replicas = 2;
    double rate = 500'000;     // commands per second; 0 = as fast as TCP allows
    double seconds = 5;        // measured window
    double warmupSeconds = 1;  // not measured
    int repeats = 3;
    uint32_t spinMicros = 0;   // receiver spin window (handwritten transport)
    size_t datagramBytes = DEFAULT_DATAGRAM_MAX_BYTES;
    int receiveBufferBytes = 4 * 1024 * 1024;
    std::vector<int> driverPids; // external media drivers to include in CPU accounting
    std::string aeronDir;      // transports that need an external driver: the gateway's
    // One driver per replica, as on separate hosts. Empty = every replica shares aeronDir's
    // driver, which also shares one received stream between them.
    std::vector<std::string> aeronReplicaDirs;
    std::string aeronChannel;  // override the transport's default channel
    std::string aeronIdle = "busy"; // receiver poll idle strategy: busy | backoff
    int stallReplica = -1;     // >= 0: this replica's consumer stops for stallMs, stallAtSeconds into the window
    double stallAtSeconds = 2.0;
    double stallMs = 1500;
    int lateStartReplica = -1; // >= 0: this replica's receiver starts lateStartMs into the window
    double lateStartMs = 2000;
};

constexpr uint16_t TCP_PORT = 47999;
constexpr const char* MCAST_GROUP = "239.255.9.9";
constexpr uint16_t MCAST_PORT = 26999;
constexpr uint16_t RETRANSMIT_PORT = 46999;
constexpr size_t STORE_SLOTS = 1u << 20;

inline NetworkConfig MakeNetworkConfig(const Options& opt) {
    NetworkConfig cfg;
    cfg.ingressListenPort = TCP_PORT;
    cfg.multicastIp = MCAST_GROUP;
    cfg.multicastPort = MCAST_PORT;
    cfg.retransmitServerIp = "127.0.0.1";
    cfg.retransmitServerPort = RETRANSMIT_PORT;
    cfg.ingressIdleProbe = false;
    cfg.ingressSpinMicros = opt.spinMicros;
    cfg.ingressDatagramMaxBytes = opt.datagramBytes;
    cfg.ingressReceiveBufferBytes = opt.receiveBufferBytes;
    return cfg;
}

// ---------------------------------------------------------------------------
// Rigs: the transport under test.
// ---------------------------------------------------------------------------

class ReceiverHandle {
public:
    virtual ~ReceiverHandle() = default;
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual bool gaveUp() const = 0;
    virtual bool ready() const { return true; } // connected to the stream and able to receive
};

class Rig {
public:
    virtual ~Rig() = default;
    virtual const char* name() const = 0;
    virtual void start() = 0;                                                  // gateway side
    virtual std::unique_ptr<ReceiverHandle> makeReceiver(SPSCQueue<BenchCommand>& queue) = 0;
    virtual void stop() = 0;
    // A transport that needs the gateway to be connected to its receivers before
    // traffic starts reports it here.
    virtual bool ready() const { return true; } // the gateway side is connected to its receivers
};

} // namespace bench
