// End-to-end ingress benchmark. Not part of the default build: configure with
// -DTOYEXCHANGE_BENCHMARKS=ON (see Benchmarks/README.md).
//
//   TCP client --> NetworkGateway --> transport --> R receivers --> R drain threads
//
// The client sends 8-byte frames at a target rate. Each frame carries the time it
// was SCHEDULED to be sent, not the time the client got round to it, so a stall
// anywhere downstream shows up as latency instead of being hidden (coordinated
// omission). A drain thread per replica stands in for the matching thread: it pops
// commands, checks that it sees every command exactly once and in order, and records
// scheduled-send-to-dequeue latency in an HdrHistogram.
//
// The transport (how the gateway's records reach the receivers) is the part under
// test. It sits behind the Rig interface so more than one implementation can run
// through the same client, the same drain threads and the same accounting.

#include "BenchRig.hpp"
#ifdef TOYEXCHANGE_AERON
#include "AeronRig.hpp"
#define TOYEXCHANGE_AERON_USAGE ", aeron-ipc, aeron-udp"
#else
#define TOYEXCHANGE_AERON_USAGE " (build with the Aeron options for aeron-ipc and aeron-udp)"
#endif
#include "NetworkGateway.hpp"
#include "MulticastIngressReceiver.hpp"
#include "RetransmitServer.hpp"
#include "SPSCQueue.hpp"

#include <hdr/hdr_histogram.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <sys/resource.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

namespace {

using namespace bench;

class HandwrittenReceiver final : public ReceiverHandle {
public:
    HandwrittenReceiver(SPSCQueue<BenchCommand>& queue, const NetworkConfig& cfg) : m_receiver(queue, cfg) {}
    void start() override { m_receiver.start(); }
    void stop() override { m_receiver.stop(); }
    bool gaveUp() const override { return m_receiver.hitUnrecoverableGap(); }
private:
    MulticastIngressReceiver<BenchCommand> m_receiver;
};

class HandwrittenRig final : public Rig {
public:
    explicit HandwrittenRig(const Options& opt)
        : m_cfg(MakeNetworkConfig(opt)),
          m_gateway(m_handler, m_cfg, StorePath(), STORE_SLOTS),
          m_retransmit(m_gateway.sequenceStore(), RETRANSMIT_PORT, opt.datagramBytes) {
    }
    const char* name() const override { return "handwritten"; }
    void start() override { m_retransmit.start(); m_gateway.start(); }
    std::unique_ptr<ReceiverHandle> makeReceiver(SPSCQueue<BenchCommand>& queue) override {
        return std::make_unique<HandwrittenReceiver>(queue, m_cfg);
    }
    void stop() override { m_gateway.stop(); m_retransmit.stop(); }

private:
    static std::string StorePath() {
        const std::string path = "/tmp/toyexchange_ingress_bench.dat";
        std::remove(path.c_str());
        return path;
    }
    NetworkConfig m_cfg;
    FrameHandler m_handler;
    NetworkGateway<FrameHandler, BenchCommand> m_gateway;
    RetransmitServer<BenchCommand> m_retransmit;
};

// ---------------------------------------------------------------------------
// Measurement
// ---------------------------------------------------------------------------

struct Replica {
    SPSCQueue<BenchCommand> queue;
    std::unique_ptr<ReceiverHandle> receiver;
    hdr_histogram* latency = nullptr;
    std::atomic<uint64_t> delivered{ 0 };   // all commands popped
    std::atomic<uint64_t> violations{ 0 };  // not exactly the next sequence number
    // The consumer pauses between these two times: a slow matching thread. It ends the
    // stall by its own clock, because nothing else can be relied on to run while the
    // transport is back-pressuring the client.
    std::atomic<uint64_t> stallStartNs{ 0 };
    std::atomic<uint64_t> stallEndNs{ 0 };
    std::thread drainer;
};

struct RunResult {
    double offeredPerSec = 0;
    double deliveredPerSec = 0;   // slowest replica
    double p50us = 0, p99us = 0, p999us = 0, maxUs = 0;
    double cpuUsPerCommand = 0;   // this process, plus the external driver if any, per command per replica
    uint64_t violations = 0;
    uint64_t missing = 0;         // sent but not delivered to the slowest replica after the drain
    bool gaveUp = false;
};

double ProcessCpuSeconds() {
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    return usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6 + usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6;
}

double PidCpuSeconds(int pid) {
    if (pid <= 0) return 0;
    char path[64];
    std::snprintf(path, sizeof(path), "/proc/%d/stat", pid);
    FILE* f = std::fopen(path, "r");
    if (!f) return 0;
    char buf[1024];
    const size_t n = std::fread(buf, 1, sizeof(buf) - 1, f);
    std::fclose(f);
    buf[n] = 0;
    const char* after = std::strrchr(buf, ')'); // the command name may contain spaces
    if (!after) return 0;
    unsigned long utime = 0, stime = 0;
    // fields after ")": state ppid pgrp session tty tpgid flags minflt cminflt majflt cmajflt utime stime
    if (std::sscanf(after + 2, "%*c %*d %*d %*d %*d %*d %*u %*u %*u %*u %*u %lu %lu", &utime, &stime) != 2) return 0;
    return static_cast<double>(utime + stime) / static_cast<double>(sysconf(_SC_CLK_TCK));
}

double DriversCpuSeconds(const std::vector<int>& pids) {
    double total = 0;
    for (const int pid : pids) total += PidCpuSeconds(pid);
    return total;
}

std::vector<std::string> SplitCommaList(const std::string& list) {
    std::vector<std::string> parts;
    size_t start = 0;
    while (start <= list.size()) {
        const size_t comma = list.find(',', start);
        const size_t end = comma == std::string::npos ? list.size() : comma;
        if (end > start) parts.push_back(list.substr(start, end - start));
        if (comma == std::string::npos) break;
        start = comma + 1;
    }
    return parts;
}

RunResult RunOnce(const Options& opt) {
    std::unique_ptr<Rig> rig;
    if (opt.transport == "handwritten") {
        rig = std::make_unique<HandwrittenRig>(opt);
    }
#ifdef TOYEXCHANGE_AERON
    else if (opt.transport == "aeron-ipc" || opt.transport == "aeron-udp") {
        rig = MakeAeronRig(opt);
    }
#endif
    else {
        std::fprintf(stderr, "unknown transport '%s'\n", opt.transport.c_str());
        std::exit(2);
    }

    std::vector<std::unique_ptr<Replica>> replicas;
    for (int i = 0; i < opt.replicas; ++i) replicas.push_back(std::make_unique<Replica>());

    std::atomic<bool> drain{ true };
    std::atomic<bool> measuring{ false };

    rig->start();
    for (auto& replica : replicas) {
        hdr_init(1, 100'000'000'000LL, 3, &replica->latency);
        replica->receiver = rig->makeReceiver(replica->queue);
        if (static_cast<int>(&replica - &replicas[0]) != opt.lateStartReplica) replica->receiver->start();
        Replica* r = replica.get();
        r->drainer = std::thread([r, &drain, &measuring] {
            BenchCommand cmd;
            uint32_t expected = 0;
            uint64_t popped = 0;
            while (drain.load(std::memory_order_relaxed)) {
                const uint64_t stallEnd = r->stallEndNs.load(std::memory_order_relaxed);
                if (stallEnd != 0) {
                    const uint64_t t = NowNs();
                    if (t >= r->stallStartNs.load(std::memory_order_relaxed) && t < stallEnd) {
                        std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        continue;
                    }
                }
                if (!r->queue.try_pop(cmd)) { _mm_pause(); continue; }
                const uint64_t now = NowNs();
                if (cmd.trace_id != expected) r->violations.fetch_add(1, std::memory_order_relaxed);
                expected = cmd.trace_id + 1;
                if (measuring.load(std::memory_order_relaxed) && now >= cmd.scheduledSendNs) {
                    hdr_record_value(r->latency, static_cast<int64_t>(now - cmd.scheduledSendNs));
                }
                r->delivered.store(++popped, std::memory_order_relaxed);
            }
        });
    }
    {
        const auto deadline = Clock::now() + std::chrono::seconds(10);
        auto allReady = [&] {
            if (!rig->ready()) return false;
            for (size_t i = 0; i < replicas.size(); ++i) {
                if (static_cast<int>(i) == opt.lateStartReplica) continue; // not started yet
                if (!replicas[i]->receiver->ready()) return false;
            }
            return true;
        };
        while (!allReady() && Clock::now() < deadline) std::this_thread::sleep_for(std::chrono::milliseconds(10));
        if (!allReady()) {
            std::fprintf(stderr, "the transport did not connect within 10 s\n");
            std::exit(1);
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300)); // let the multicast joins settle

    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(TCP_PORT);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        std::perror("connect to gateway");
        std::exit(1);
    }

    // Open-loop client: frame k is due at t0 + k / rate. Whatever is due and not yet
    // sent goes out in one write, up to 8192 frames.
    std::vector<uint64_t> frames(8192);
    uint64_t sent = 0;
    const uint64_t t0 = NowNs();
    const uint64_t warmupEnd = t0 + static_cast<uint64_t>(opt.warmupSeconds * 1e9);
    const uint64_t end = warmupEnd + static_cast<uint64_t>(opt.seconds * 1e9);
    uint64_t sentAtWindowStart = 0, sentAtWindowEnd = 0;
    std::vector<uint64_t> deliveredAtStart(opt.replicas, 0);
    double cpuStart = 0, driverCpuStart = 0;
    bool windowOpen = false;
    bool lateStarted = false;

    while (true) {
        const uint64_t now = NowNs();
        if (!windowOpen && now >= warmupEnd) {
            windowOpen = true;
            sentAtWindowStart = sent;
            for (size_t i = 0; i < replicas.size(); ++i) deliveredAtStart[i] = replicas[i]->delivered.load();
            for (auto& replica : replicas) hdr_reset(replica->latency);
            cpuStart = ProcessCpuSeconds();
            driverCpuStart = DriversCpuSeconds(opt.driverPids);
            measuring.store(true);
            if (opt.stallReplica >= 0 && opt.stallReplica < opt.replicas) {
                const uint64_t stallStart = warmupEnd + static_cast<uint64_t>(opt.stallAtSeconds * 1e9);
                replicas[opt.stallReplica]->stallStartNs.store(stallStart);
                replicas[opt.stallReplica]->stallEndNs.store(stallStart + static_cast<uint64_t>(opt.stallMs * 1e6));
            }
        }
        if (now >= end) break;

        if (opt.lateStartReplica >= 0 && !lateStarted && now >= warmupEnd + static_cast<uint64_t>(opt.lateStartMs * 1e6)) {
            replicas[opt.lateStartReplica]->receiver->start(); // joins a stream that is already well under way
            lateStarted = true;
        }

        size_t chunk = frames.size();
        if (opt.rate > 0) {
            const uint64_t due = static_cast<uint64_t>(opt.rate * static_cast<double>(now - t0) / 1e9);
            if (sent >= due) { _mm_pause(); continue; }
            chunk = std::min<size_t>(chunk, due - sent);
        }
        for (size_t i = 0; i < chunk; ++i) {
            frames[i] = opt.rate > 0
                ? t0 + static_cast<uint64_t>(static_cast<double>(sent + i) * 1e9 / opt.rate)
                : now;
        }
        const ssize_t n = send(fd, frames.data(), chunk * FRAME_BYTES, MSG_NOSIGNAL);
        if (n > 0) sent += static_cast<uint64_t>(n) / FRAME_BYTES;
        // A short write leaves the tail of `frames` unsent; those frames are rebuilt on the
        // next pass from the same schedule, so the stamps stay correct.
    }
    sentAtWindowEnd = sent;
    const double cpuEnd = ProcessCpuSeconds();
    const double driverCpuEnd = DriversCpuSeconds(opt.driverPids);
    std::vector<uint64_t> deliveredAtEnd(opt.replicas, 0);
    for (size_t i = 0; i < replicas.size(); ++i) deliveredAtEnd[i] = replicas[i]->delivered.load();
    measuring.store(false);

    // Let the receivers work off whatever is still in flight, then count what is missing.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    RunResult result;
    const double window = opt.seconds;
    result.offeredPerSec = static_cast<double>(sentAtWindowEnd - sentAtWindowStart) / window;
    uint64_t slowest = UINT64_MAX;
    hdr_histogram* merged = nullptr;
    hdr_init(1, 100'000'000'000LL, 3, &merged);
    for (size_t i = 0; i < replicas.size(); ++i) {
        slowest = std::min(slowest, deliveredAtEnd[i] - deliveredAtStart[i]);
        result.violations += replicas[i]->violations.load();
        result.gaveUp = result.gaveUp || replicas[i]->receiver->gaveUp();
        result.missing = std::max<uint64_t>(result.missing, sent - replicas[i]->delivered.load());
        hdr_add(merged, replicas[i]->latency);
    }
    result.deliveredPerSec = static_cast<double>(slowest) / window;
    if (opt.stallReplica >= 0 || opt.lateStartReplica >= 0) {
        for (size_t i = 0; i < replicas.size(); ++i) {
            const char* tag = static_cast<int>(i) == opt.stallReplica ? " (stalled)"
                : static_cast<int>(i) == opt.lateStartReplica ? " (late start)" : "";
            std::printf("#   replica %zu%s: delivered %.0f/s in the window, p50 %.1f us, p99 %.1f us, max %.1f us, "
                "%" PRIu64 " still missing after 1 s, %" PRIu64 " out-of-sequence, %s\n",
                i, tag,
                static_cast<double>(deliveredAtEnd[i] - deliveredAtStart[i]) / window,
                hdr_value_at_percentile(replicas[i]->latency, 50.0) / 1e3,
                hdr_value_at_percentile(replicas[i]->latency, 99.0) / 1e3,
                static_cast<double>(hdr_max(replicas[i]->latency)) / 1e3,
                sent - replicas[i]->delivered.load(), replicas[i]->violations.load(),
                replicas[i]->receiver->gaveUp() ? "GAVE UP" : "still running");
        }
    }
    result.p50us = hdr_value_at_percentile(merged, 50.0) / 1e3;
    result.p99us = hdr_value_at_percentile(merged, 99.0) / 1e3;
    result.p999us = hdr_value_at_percentile(merged, 99.9) / 1e3;
    result.maxUs = static_cast<double>(hdr_max(merged)) / 1e3;
    const double commands = static_cast<double>(slowest) * opt.replicas;
    if (commands > 0) {
        result.cpuUsPerCommand = ((cpuEnd - cpuStart) + (driverCpuEnd - driverCpuStart)) * 1e6 / commands;
    }

    close(fd);
    drain.store(false);
    for (auto& replica : replicas) replica->drainer.join();
    for (auto& replica : replicas) replica->receiver->stop();
    rig->stop();
    for (auto& replica : replicas) hdr_close(replica->latency);
    hdr_close(merged);
    return result;
}

double Median(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    return v.empty() ? 0 : v[v.size() / 2];
}

void Usage() {
    std::puts("IngressBench [--transport handwritten] [--replicas N] [--rate CMDS_PER_SEC|0] [--seconds S]\n"
              "             [--warmup S] [--repeats K] [--spin-us N] [--datagram-bytes N] [--receive-buffer N]\n"
              "             [--driver-pid PID[,PID...]] [--aeron-dir DIR] [--aeron-replica-dirs DIR[,DIR...]]\n"
              "             [--aeron-channel URI] [--aeron-idle busy|backoff]\n"
              "             [--stall-replica N --stall-at S --stall-ms MS]\n"
              "transports: handwritten" TOYEXCHANGE_AERON_USAGE);
}

} // namespace

int main(int argc, char** argv) {
    Options opt;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto next = [&]() -> const char* {
            if (i + 1 >= argc) { Usage(); std::exit(2); }
            return argv[++i];
        };
        if (arg == "--transport") opt.transport = next();
        else if (arg == "--replicas") opt.replicas = std::atoi(next());
        else if (arg == "--rate") opt.rate = std::atof(next());
        else if (arg == "--seconds") opt.seconds = std::atof(next());
        else if (arg == "--warmup") opt.warmupSeconds = std::atof(next());
        else if (arg == "--repeats") opt.repeats = std::atoi(next());
        else if (arg == "--spin-us") opt.spinMicros = static_cast<uint32_t>(std::atoi(next()));
        else if (arg == "--datagram-bytes") opt.datagramBytes = static_cast<size_t>(std::atoll(next()));
        else if (arg == "--receive-buffer") opt.receiveBufferBytes = std::atoi(next());
        else if (arg == "--driver-pid") {
            for (const std::string& pid : SplitCommaList(next())) opt.driverPids.push_back(std::atoi(pid.c_str()));
        }
        else if (arg == "--aeron-replica-dirs") opt.aeronReplicaDirs = SplitCommaList(next());
        else if (arg == "--aeron-dir") opt.aeronDir = next();
        else if (arg == "--aeron-channel") opt.aeronChannel = next();
        else if (arg == "--aeron-idle") opt.aeronIdle = next();
        else if (arg == "--stall-replica") opt.stallReplica = std::atoi(next());
        else if (arg == "--stall-at") opt.stallAtSeconds = std::atof(next());
        else if (arg == "--stall-ms") opt.stallMs = std::atof(next());
        else if (arg == "--late-start-replica") opt.lateStartReplica = std::atoi(next());
        else if (arg == "--late-start-ms") opt.lateStartMs = std::atof(next());
        else { Usage(); return 2; }
    }

    std::printf("# transport=%s replicas=%d rate=%.0f/s window=%.1fs warmup=%.1fs spin=%uus datagram=%zuB record=%zuB\n",
        opt.transport.c_str(), opt.replicas, opt.rate, opt.seconds, opt.warmupSeconds, opt.spinMicros,
        opt.datagramBytes, sizeof(BenchMessage));
    std::printf("# run  offered/s  delivered/s(slowest)  p50us  p99us  p99.9us  maxus  cpu_us/cmd  violations  missing  gaveup\n");

    std::vector<double> offered, delivered, p50, p99, p999, maxUs, cpu;
    uint64_t violations = 0, missing = 0;
    bool gaveUp = false;
    for (int run = 1; run <= opt.repeats; ++run) {
        // A transport that blocks the gateway for good would leave this run waiting forever.
        // That is a result too: report it and stop.
        std::atomic<bool> runFinished{ false };
        std::thread watchdog([&] {
            const auto deadline = Clock::now() + std::chrono::duration<double>(opt.warmupSeconds + opt.seconds + 30);
            while (!runFinished.load() && Clock::now() < deadline) std::this_thread::sleep_for(std::chrono::milliseconds(100));
            if (!runFinished.load()) {
                std::printf("HUNG: run %d did not finish within 30 s of its end; the gateway is blocked and is not recovering\n", run);
                std::fflush(stdout);
                _exit(3);
            }
        });
        const RunResult r = RunOnce(opt);
        runFinished.store(true);
        watchdog.join();
        std::printf("%4d  %9.0f  %9.0f  %7.1f  %7.1f  %7.1f  %9.1f  %6.2f  %" PRIu64 "  %" PRIu64 "  %s\n",
            run, r.offeredPerSec, r.deliveredPerSec, r.p50us, r.p99us, r.p999us, r.maxUs, r.cpuUsPerCommand,
            r.violations, r.missing, r.gaveUp ? "YES" : "no");
        std::fflush(stdout);
        offered.push_back(r.offeredPerSec); delivered.push_back(r.deliveredPerSec);
        p50.push_back(r.p50us); p99.push_back(r.p99us); p999.push_back(r.p999us); maxUs.push_back(r.maxUs);
        cpu.push_back(r.cpuUsPerCommand);
        violations += r.violations; missing += r.missing; gaveUp = gaveUp || r.gaveUp;
    }
    std::printf("median  %9.0f  %9.0f  %7.1f  %7.1f  %7.1f  %9.1f  %6.2f  %" PRIu64 "  %" PRIu64 "  %s\n",
        Median(offered), Median(delivered), Median(p50), Median(p99), Median(p999), Median(maxUs), Median(cpu),
        violations, missing, gaveUp ? "YES" : "no");
    return (violations == 0 && missing == 0 && !gaveUp) ? 0 : 1;
}
