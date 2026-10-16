#pragma once
#include <cstddef>
#include <cstdint>
#include <string>

// Default payload limit for one datagram on the ingress path. A datagram this
// size fits a 1500-byte Ethernet MTU with room for the IP and UDP headers, so
// the network never fragments it. Used by NetworkConfig and, as the default of
// its constructor, by RetransmitServer.
inline constexpr size_t DEFAULT_DATAGRAM_MAX_BYTES = 1400;

// The largest payload a UDP datagram can carry. Receive buffers are this big, whatever the sender's limit.
inline constexpr size_t MAX_UDP_PAYLOAD_BYTES = 65535;

struct NetworkConfig {
    // Inbound order-entry configuration — whatever wire protocol the
    // product built on this pipeline uses (e.g. OUCH for the event-contract
    // exchange).
    uint16_t ingressListenPort = 10001;

    // Public market-data broadcast (UDP multicast group) — whatever wire
    // format the product uses for it (e.g. this project's own compact
    // frame today, or a real ITCH feed).
    std::string publicBroadcastIp = "233.0.1.1";
    uint16_t publicBroadcastPort = 20001;

    // Internal order-replication channel: NetworkGateway multicasts each
    // sequenced SequencedInboundMessage<TCommand> here, and every
    // MatchingService replica's MulticastIngressReceiver joins the same
    // group/port to consume it. Not the public broadcast feed (see above).
    std::string multicastIp = "239.10.10.10";
    uint16_t multicastPort = 30001;

    // Gap-fill / retransmit recovery (queried by MulticastIngressReceiver)
    std::string retransmitServerIp = "127.0.0.1";
    uint16_t retransmitServerPort = 40001;

    // After a full receive timeout with no datagram, MulticastIngressReceiver
    // asks the retransmit server whether it missed anything, so a replica that
    // just (re)started or lost the tail of a burst catches up without waiting
    // for the next live command. false = only a live datagram that reveals a
    // sequence gap triggers recovery.
    bool ingressIdleProbe = true;

    // NetworkGateway sends every command it parsed in one epoll round as a few
    // datagrams of back-to-back records, not one datagram per command. A datagram
    // holds as many whole records as fit in this many bytes (at least one).
    // Waking a receiver costs the sender the same whatever the datagram size, so
    // more records per datagram means more commands per second. Keep it at or
    // below the path MTU minus 28 bytes, or the network fragments the datagram
    // and one lost fragment loses every record in it. On a single host, where
    // the path MTU is 65,536, a larger value is safe.
    size_t ingressDatagramMaxBytes = DEFAULT_DATAGRAM_MAX_BYTES;

    // Receiver spin window, in microseconds. 0 (the default) means
    // MulticastIngressReceiver blocks in recv() between datagrams. A value N
    // above 0 means that after each datagram it polls the socket for up to N
    // microseconds before it blocks again. A sleeping receiver costs a
    // co-located sender a wake-up for every datagram, so a product that must
    // sustain a very high command rate with several replicas on one host can
    // turn this on (50 to 100 is a good start). The price is one busy thread per
    // replica for as long as traffic keeps arriving. An idle receiver never
    // spins.
    uint32_t ingressSpinMicros = 0;

    // Kernel receive buffer that MulticastIngressReceiver asks for, on both the
    // multicast socket and the retransmit-reply socket. The Linux default is
    // 212,992 bytes, and a burst of commands can overflow it while the receiver
    // thread waits for room in a full inbound queue. Every datagram lost that
    // way forces a retransmit. Linux caps the request at net.core.rmem_max, so
    // raise that sysctl on the host to at least this value; the receiver prints
    // a warning at start-up when the cap clips the request.
    int ingressReceiveBufferBytes = 4 * 1024 * 1024;

    // Internal results-egress channel: each MatchingService replica's
    // EgressPublisher multicasts every raw event it produces here, and
    // NetworkServiceApp's EgressGateway joins it to derive both the private
    // ack and the public broadcast (above). No gap-fill/retransmit on this
    // channel for now — see EgressPublisher.hpp.
    std::string egressMulticastIp = "239.10.10.20";
    uint16_t egressMulticastPort = 30002;

    // Egress batching. The publisher sends whatever events its queue holds at that moment, as one
    // datagram of at most this many bytes (a header, then whole events). It never waits for more.
    // The gateway also packs the frames of the public broadcast into datagrams of at most this size.
    // The same fragmentation rule applies as for ingressDatagramMaxBytes.
    size_t egressDatagramMaxBytes = DEFAULT_DATAGRAM_MAX_BYTES;

    // Kernel receive buffer for the egress gateway's socket, as ingressReceiveBufferBytes is for the
    // receivers. Egress datagrams that overflow it are lost and are not recovered. The gateway
    // prints a warning at start-up when net.core.rmem_max clips the request, and another (at most once
    // a second) when the ordinals show that events never arrived.
    int egressReceiveBufferBytes = 4 * 1024 * 1024;

    // Spin window of the egress gateway's receive, in microseconds. As ingressSpinMicros: 0 blocks,
    // N above 0 polls for up to N microseconds after each datagram before it blocks again.
    uint32_t egressSpinMicros = 0;

    // true: EgressGateway prints one "[egress] ..." line per event to standard output, written once per
    // batch. A product's tests may count those lines. false: nothing is printed, which saves the formatting
    // and the write on the egress thread. Switch it off for load. On the test machine the thread, with
    // printing on, saturated near 1,000,000 events/s and the gateway lost about one event in five; with
    // printing off it used about half a core at the same rate and lost none.
    bool egressConsoleEcho = true;

    // Largest amount of not-yet-sent private output the gateway keeps for one client whose socket is
    // full. Past it, further output for that client is dropped whole, not cut mid-frame, and counted.
    size_t sessionOutboundBufferBytes = 1024 * 1024;

    // Matching-replica leader arbitration: NetworkGateway's ReplicaArbiter
    // listens here for replica heartbeats and replies with the current
    // {epoch, leaderId} — see Core/Arbitration/. Only one replica is
    // allowed to actually publish egress at a time.
    uint16_t arbitrationPort = 40003;

    // Internal telemetry channel: each MatchingService replica's
    // TelemetryForwarder unicasts a TelemetryReport per completed OrderTrace
    // here; PerformanceServiceApp binds this port to build one canonical,
    // centralized view of every replica's engine performance. Point-to-point
    // (not multicast) since there is exactly one intended receiver.
    std::string performanceServiceIp = "127.0.0.1";
    uint16_t performanceServicePort = 40002;
};
