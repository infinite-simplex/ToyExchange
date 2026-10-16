#pragma once
// Shared by the Core tests that talk to a real ReplicaArbiter (Testing.cpp, EgressTesting.cpp).

#include "HeartbeatMessage.hpp"

#include <cstdint>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace arbiter_test {

// One heartbeat from `id`, sent the way a replica sends it, and the arbiter's reply.
// The reply is zeroed if the arbiter does not answer within 500 ms.
inline HeartbeatResponse SendHeartbeat(uint16_t arbiterPort, ReplicaId id, uint64_t lastAppliedSeq) {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    timeval rxTimeout{};
    rxTimeout.tv_sec = 0;
    rxTimeout.tv_usec = 500'000;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &rxTimeout, sizeof(rxTimeout));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(arbiterPort);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    HeartbeatRequest req{ id, lastAppliedSeq };
    sendto(sock, &req, sizeof(req), 0, (sockaddr*)&addr, sizeof(addr));

    HeartbeatResponse resp{};
    recv(sock, &resp, sizeof(resp), 0);
    close(sock);
    return resp;
}

} // namespace arbiter_test
