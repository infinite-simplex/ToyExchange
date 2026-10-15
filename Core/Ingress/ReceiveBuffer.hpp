#pragma once
#include <sys/socket.h>

// Asks the kernel for a receive buffer of requestedBytes on a UDP socket and
// returns the size it granted, or 0 if the query fails.
//
// Linux caps the request at net.core.rmem_max without any error, and the
// default cap is 212,992 bytes. It also stores twice the granted value for its
// own bookkeeping, so getsockopt reports double. This function halves that
// value, so the result compares directly with the request. A result below the
// request means the cap clipped it.
inline int SetReceiveBuffer(int fd, int requestedBytes) {
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &requestedBytes, sizeof(requestedBytes));

    int stored = 0;
    socklen_t len = sizeof(stored);
    if (getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &stored, &len) < 0) return 0;
    return stored / 2;
}
