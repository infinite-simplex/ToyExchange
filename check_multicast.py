#!/usr/bin/env python3
"""
Quick multicast reachability check between machines -- run this BEFORE
attempting a multi-machine exchange deployment, to isolate "is multicast
actually reaching this network path" from "is something wrong in the
exchange's own code."

A successful `ping` only proves ordinary unicast IP reachability; it says
nothing about whether multicast group traffic (which the replication,
egress, and ITCH channels all depend on -- see NetworkService/NetworkConfig.hpp)
actually gets delivered. That depends on IGMP joins being honored by
whatever switches/routers/APs sit between the machines, which is a
genuinely different and less reliable path than ordinary unicast --
especially over WiFi, where multicast frames get no MAC-layer ACK/retry.

Usage:
  Receiver (run first, on each laptop / every machine that should receive):
    python3 check_multicast.py recv [--group 239.10.10.10] [--port 30001]

  Sender (run on the machine that will host NetworkServiceApp):
    python3 check_multicast.py send [--group 239.10.10.10] [--port 30001]

Defaults match NetworkConfig's actual replication multicast channel, so a
pass here specifically confirms the channel MatchingServiceApp's
MulticastIngressReceiver depends on. Re-run with --group/--port set to the
egress channel (239.10.10.20:30002) or the ITCH channel (233.0.1.1:20001)
if you want to check those specifically too, though if the replication
channel works, the others almost always do -- same network path, same
class of traffic.
"""
import argparse
import socket
import struct
import sys
import time


def recv(group: str, port: int) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("", port))
    mreq = struct.pack("4sl", socket.inet_aton(group), socket.INADDR_ANY)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)

    print(f"Listening for multicast on {group}:{port} ... (Ctrl+C to stop)")
    count = 0
    try:
        while True:
            data, addr = s.recvfrom(1024)
            count += 1
            print(f"[{count}] received {len(data)} bytes from {addr[0]}: {data!r}")
    except KeyboardInterrupt:
        print(f"\nStopped. Received {count} packet(s) total.")
        if count == 0:
            print("FAIL: no multicast packets ever arrived. Most likely causes: IGMP")
            print("snooping on a switch/router/AP not forwarding to this join, WiFi")
            print("client/AP isolation blocking the traffic outright, a mesh network's")
            print("backhaul not carrying multicast between nodes, or a host firewall")
            print("dropping it.")
            sys.exit(1)


def send(group: str, port: int) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    ttl = struct.pack("b", 1)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)

    print(f"Sending multicast to {group}:{port} once per second (Ctrl+C to stop)...")
    i = 0
    try:
        while True:
            i += 1
            s.sendto(f"multicast_check #{i}".encode(), (group, port))
            print(f"sent #{i}")
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\nStopped after sending {i} packet(s).")


def main() -> None:
    # Unbuffer stdout: Python fully-buffers when stdout isn't a TTY (e.g.
    # redirected to a log file for `tail -f`), so without this, nothing
    # shows up until the buffer fills or the process exits -- same reasoning
    # as std::cout.setf(std::ios_base::unitbuf) in this repo's C++ services.
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["send", "recv"])
    parser.add_argument("--group", default="239.10.10.10")
    parser.add_argument("--port", type=int, default=30001)
    args = parser.parse_args()

    if args.mode == "recv":
        recv(args.group, args.port)
    else:
        send(args.group, args.port)


if __name__ == "__main__":
    main()
