#pragma once
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <emmintrin.h>
#include <sys/socket.h>

// Reads one datagram from `fd` and returns recv()'s result (the length, or -1 with errno set).
//
// With `spin` true and spinMicros above 0, it first polls without blocking for up to spinMicros, so a
// sender that is already waiting does not have to wake this thread. A co-located sender pays for every
// wake-up it causes, so at a high rate this is the difference between about 340,000 and 500,000
// commands per second. After the spin window it blocks in recv(), which honours the socket's receive
// timeout. Callers pass spin = true only right after traffic, so an idle receiver never spins.
inline ssize_t ReceiveDatagram(int fd, char* buffer, size_t capacity, bool spin, uint32_t spinMicros) {
    if (spin && spinMicros > 0) {
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(spinMicros);
        do {
            const ssize_t len = recv(fd, buffer, capacity, MSG_DONTWAIT);
            if (len >= 0 || (errno != EAGAIN && errno != EWOULDBLOCK)) return len;
            _mm_pause();
        } while (std::chrono::steady_clock::now() < deadline);
    }
    return recv(fd, buffer, capacity, 0);
}
