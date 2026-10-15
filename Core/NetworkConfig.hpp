#pragma once
#include <string>

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
