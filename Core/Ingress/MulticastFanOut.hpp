#pragma once
#include "NetworkConfig.hpp"

#include <cerrno>
#include <cstring>
#include <system_error>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/udp.h>
#include <sys/socket.h>
#include <unistd.h>

#ifndef UDP_SEGMENT
#define UDP_SEGMENT 103
#endif
#ifndef SOL_UDP
#define SOL_UDP 17
#endif

// How NetworkGateway gets its sequenced records to the replicas: one UDP
// multicast send per call to publish(). This is the default fan-out.
//
// A fan-out is any type with this shape:
//   explicit TFanOut(const NetworkConfig&);
//   void start();                                     // may throw
//   void stop();
//   void publish(const void* records, size_t bytes);  // whole records, back to back
//
// publish() runs on the gateway's ingress thread, once per batch, after every
// record in the batch is already in the sequence store. It must not block for
// long, and a failed send must lose no command: the store still holds it, and
// the replicas recover it.
//
// A fan-out may also provide
//   void setSegmentBytes(size_t bytes);               // after start(); may throw
// to say it cuts one publish() into datagrams of `bytes` (whole records). Only
// then does NetworkConfig::ingressGsoSegments let a batch span several datagrams.
class MulticastFanOut {
public:
    explicit MulticastFanOut(const NetworkConfig& config) : m_config(config) {}
    ~MulticastFanOut() { stop(); }

    MulticastFanOut(const MulticastFanOut&) = delete;
    MulticastFanOut& operator=(const MulticastFanOut&) = delete;

    void start() {
        m_fd = socket(AF_INET, SOCK_DGRAM, 0);
        if (m_fd < 0) {
            throw std::system_error(errno, std::generic_category(), "Unable to create multicast socket");
        }
        unsigned char ttl = 1;
        setsockopt(m_fd, IPPROTO_IP, IP_MULTICAST_TTL, &ttl, sizeof(ttl));

        std::memset(&m_dest, 0, sizeof(m_dest));
        m_dest.sin_family = AF_INET;
        m_dest.sin_port = htons(m_config.multicastPort);
        inet_pton(AF_INET, m_config.multicastIp.c_str(), &m_dest.sin_addr);
    }

    void stop() {
        if (m_fd != -1) close(m_fd);
        m_fd = -1;
    }

    // Lets one publish() carry several datagrams: the kernel cuts the buffer into
    // datagrams of `bytes` (the last may be shorter). `bytes` must be a whole number of
    // records, so no record is split across datagrams. Call after start().
    void setSegmentBytes(size_t bytes) {
        int segment = static_cast<int>(bytes);
        if (setsockopt(m_fd, SOL_UDP, UDP_SEGMENT, &segment, sizeof(segment)) < 0) {
            throw std::system_error(errno, std::generic_category(), "Unable to enable UDP segmentation offload");
        }
    }

    void publish(const void* records, size_t bytes) {
        sendto(m_fd, records, bytes, 0, reinterpret_cast<const sockaddr*>(&m_dest), sizeof(m_dest));
    }

private:
    NetworkConfig m_config;
    int m_fd = -1;
    sockaddr_in m_dest{};
};
