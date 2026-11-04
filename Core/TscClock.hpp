#pragma once
// Calibrated-TSC clock: CLOCK_TAI nanoseconds for the price of one unfenced rdtsc, a seqlock read and a multiply.
//
// The hot path (now_ns) reads a small model {tsc0, ns0, mult} and extrapolates:
//     ns = ns0 + ((rdtsc - tsc0) * mult) >> 32          mult = ns-per-tick in 32.32 fixed point
// A background thread refreshes the model every `refresh_ms` from a tightly bracketed pair
// (rdtsc, clock_gettime(CLOCK_TAI), rdtsc) and publishes it through a seqlock, so the hot path never
// makes a system call, never takes a lock and never waits for the calibrator.
//
// What the calibrator does with each new pair:
//   * error  e = (what the model said for that tick) - (what the system clock says). This is measured on every
//     refresh and is the model's accumulated error over the interval that just ended; it is exported in Stats so
//     the bound is observable at run time.
//   * |e| below `step_threshold_ns`: a slew. The new model starts where the old one stood (so the clock never
//     jumps and never runs backwards) and its rate is the slope measured over the last `slope_window` intervals,
//     minus e spread over one interval, so the model converges on the system clock within a refresh.
//   * |e| above the threshold: a step (clock_settime, a TAI offset being set by ptp4l/chrony, a leap event).
//     The model re-bases at once on the system clock, as the vDSO read would; the event is counted.
//   * a pair whose bracket is wider than `max_bracket_ns` (the calibrator was preempted mid-sample) is discarded.
//
// Falls back to clock_gettime(CLOCK_TAI) -- exactly today's behavior -- when the TSC is not invariant, the kernel
// clocksource is not tsc, or the clock has not finished its first calibration.
//
// Time source is a template parameter so the logic can be tested with a scripted clock (see tscclock_test.cpp).
#include <x86intrin.h>
#include <cpuid.h>
#include <time.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <string>
#include <thread>

namespace tscclock {

struct Config {
    unsigned refresh_ms = 10;            // calibration interval
    unsigned init_ms = 20;               // time between the two samples of the first calibration
    unsigned brackets = 8;               // (rdtsc, clock, rdtsc) triples per sample; the tightest wins
    int64_t max_bracket_ns = 2'000;      // discard a sample whose best bracket is wider than this
    unsigned slope_window = 4;           // the rate is measured over this many intervals
    int64_t step_threshold_ns = 100'000; // |error| above this is a step, not a slew
    double max_slew_ppm = 500.0;         // the model never runs faster/slower than the measured rate by more than this
};

struct Stats {
    uint64_t refreshes{ 0 };
    uint64_t skipped_wide_bracket{ 0 };
    uint64_t steps{ 0 };
    int64_t last_error_ns{ 0 };      // model minus system clock, at the last refresh
    int64_t max_abs_error_ns{ 0 };   // largest |error| at a refresh since reset_max_error()
    double ns_per_tick{ 0 };
    bool using_tsc{ false };
};

// The real clocks.
struct SystemSource {
    uint64_t tsc() noexcept { return __rdtsc(); }                                   // hot path: unfenced
    uint64_t tsc_ordered() noexcept { _mm_lfence(); const uint64_t t = __rdtsc(); _mm_lfence(); return t; } // calibration
    int64_t tai_ns() noexcept {
        timespec ts;
        clock_gettime(CLOCK_TAI, &ts);
        return static_cast<int64_t>(ts.tv_sec) * 1'000'000'000ll + ts.tv_nsec;
    }
    void sleep_ms(unsigned ms) { std::this_thread::sleep_for(std::chrono::milliseconds(ms)); }
    bool tsc_usable() const {
        unsigned a = 0, b = 0, c = 0, d = 0;
        if (!__get_cpuid(0x80000007, &a, &b, &c, &d) || !(d & (1u << 8))) return false; // invariant TSC
        std::ifstream f("/sys/devices/system/clocksource/clocksource0/current_clocksource");
        std::string cs;
        return (f >> cs) && cs == "tsc";
    }
};

template <class Source = SystemSource>
class ClockT {
public:
    explicit ClockT(Source src = Source{}, Config cfg = {}, bool run_thread = true)
        : src_(std::move(src)), cfg_(cfg) {
        if (!src_.tsc_usable()) return; // mode_ stays 0: every read is the system clock
        calibrate_first();
        if (run_thread) thread_ = std::thread([this] { run(); });
    }
    ~ClockT() { stop(); }
    ClockT(const ClockT&) = delete;
    ClockT& operator=(const ClockT&) = delete;

    void stop() {
        stop_.store(true, std::memory_order_relaxed);
        if (thread_.joinable()) thread_.join();
    }

    // TAI nanoseconds. No system call, no lock, no wait.
    [[nodiscard]] uint64_t now_ns() noexcept {
        if (mode_.load(std::memory_order_relaxed) == 0) return static_cast<uint64_t>(src_.tai_ns());
        return convert(src_.tsc());
    }

    // The same conversion for a tick the caller already read (used to compare against ground truth).
    [[nodiscard]] uint64_t convert(uint64_t tsc) const noexcept {
        uint64_t t0, n0, mult;
        uint32_t s1, s2;
        do {
            s1 = seq_.load(std::memory_order_acquire);
            t0 = tsc0_.load(std::memory_order_relaxed);
            n0 = ns0_.load(std::memory_order_relaxed);
            mult = mult_.load(std::memory_order_relaxed);
            std::atomic_thread_fence(std::memory_order_acquire);
            s2 = seq_.load(std::memory_order_relaxed);
        } while ((s1 & 1u) || s1 != s2);
        // Signed: a core's TSC may sit a few ticks behind the tick the model was anchored at.
        const int64_t delta = static_cast<int64_t>(tsc - t0);
        return n0 + static_cast<uint64_t>((static_cast<__int128>(delta) * static_cast<__int128>(mult)) >> 32);
    }

    // One calibration step. Called by the background thread; tests call it directly.
    void refresh() {
        if (mode_.load(std::memory_order_relaxed) == 0) return;
        const Sample s = take_sample();
        if (static_cast<double>(s.width_ticks) * slope_ > static_cast<double>(cfg_.max_bracket_ns)) {
            skipped_.fetch_add(1, std::memory_order_relaxed);
            return;
        }
        const int64_t predicted = static_cast<int64_t>(eval(cur_, s.tsc));
        const int64_t e = predicted - s.tai;
        refreshes_.fetch_add(1, std::memory_order_relaxed);
        last_error_.store(e, std::memory_order_relaxed);
        const int64_t ae = e < 0 ? -e : e;
        if (ae > max_abs_error_.load(std::memory_order_relaxed)) max_abs_error_.store(ae, std::memory_order_relaxed);

        if (ae > cfg_.step_threshold_ns) { // the system clock jumped: follow it, as the vDSO would
            steps_.fetch_add(1, std::memory_order_relaxed);
            hist_.clear();
            hist_.push_back(s);
            cur_ = { s.tsc, static_cast<uint64_t>(s.tai), to_mult(slope_) };
            publish(cur_);
            return;
        }
        hist_.push_back(s);
        while (hist_.size() > cfg_.slope_window + 1) hist_.pop_front();
        double slope_est = slope_;
        if (hist_.size() >= 2) {
            const Sample& o = hist_.front();
            slope_est = static_cast<double>(s.tai - o.tai) / static_cast<double>(s.tsc - o.tsc);
        }
        if (std::fabs(slope_est / slope_ - 1.0) > 0.01) slope_est = slope_; // not plausible for a TSC: ignore
        // Converge on the system clock within one interval without moving the clock backwards.
        const double interval_ticks = static_cast<double>(cfg_.refresh_ms) * 1e6 / slope_est;
        double corr = static_cast<double>(e) / interval_ticks;                       // ns per tick
        const double lim = cfg_.max_slew_ppm * 1e-6 * slope_est;
        corr = std::clamp(corr, -lim, lim);
        slope_ = slope_est;
        cur_ = { s.tsc, static_cast<uint64_t>(predicted), to_mult(slope_est - corr) };
        publish(cur_);
    }

    [[nodiscard]] Stats stats() const noexcept {
        Stats st;
        st.refreshes = refreshes_.load(std::memory_order_relaxed);
        st.skipped_wide_bracket = skipped_.load(std::memory_order_relaxed);
        st.steps = steps_.load(std::memory_order_relaxed);
        st.last_error_ns = last_error_.load(std::memory_order_relaxed);
        st.max_abs_error_ns = max_abs_error_.load(std::memory_order_relaxed);
        st.ns_per_tick = static_cast<double>(mult_.load(std::memory_order_relaxed)) / 4294967296.0;
        st.using_tsc = mode_.load(std::memory_order_relaxed) != 0;
        return st;
    }
    void reset_max_error() noexcept { max_abs_error_.store(0, std::memory_order_relaxed); }
    Source& source() noexcept { return src_; }

private:
    struct Sample { uint64_t tsc; int64_t tai; uint64_t width_ticks; };
    struct Model { uint64_t tsc0; uint64_t ns0; uint64_t mult; };

    static uint64_t to_mult(double ns_per_tick) { return static_cast<uint64_t>(std::llround(ns_per_tick * 4294967296.0)); }
    static uint64_t eval(const Model& m, uint64_t tsc) {
        const int64_t delta = static_cast<int64_t>(tsc - m.tsc0);
        return m.ns0 + static_cast<uint64_t>((static_cast<__int128>(delta) * static_cast<__int128>(m.mult)) >> 32);
    }

    Sample take_sample() {
        Sample best{ 0, 0, ~uint64_t{ 0 } };
        for (unsigned i = 0; i < cfg_.brackets; ++i) {
            const uint64_t a = src_.tsc_ordered();
            const int64_t t = src_.tai_ns();
            const uint64_t b = src_.tsc_ordered();
            const uint64_t w = b - a;
            if (w < best.width_ticks) best = { a + w / 2, t, w };
        }
        return best;
    }

    void calibrate_first() {
        const Sample s0 = take_sample();
        src_.sleep_ms(cfg_.init_ms);
        const Sample s1 = take_sample();
        slope_ = static_cast<double>(s1.tai - s0.tai) / static_cast<double>(s1.tsc - s0.tsc);
        if (!(slope_ > 0.0) || !std::isfinite(slope_)) return; // implausible: stay on the system clock
        hist_.push_back(s0);
        hist_.push_back(s1);
        cur_ = { s1.tsc, static_cast<uint64_t>(s1.tai), to_mult(slope_) };
        publish(cur_);
        mode_.store(1, std::memory_order_release);
    }

    void publish(const Model& m) {
        const uint32_t s = seq_.load(std::memory_order_relaxed);
        seq_.store(s + 1, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_release);
        tsc0_.store(m.tsc0, std::memory_order_relaxed);
        ns0_.store(m.ns0, std::memory_order_relaxed);
        mult_.store(m.mult, std::memory_order_relaxed);
        seq_.store(s + 2, std::memory_order_release);
    }

    void run() {
        while (!stop_.load(std::memory_order_relaxed)) {
            src_.sleep_ms(cfg_.refresh_ms);
            refresh();
        }
    }

    Source src_;
    Config cfg_;
    // Hot-path state, one cache line, written by the calibrator every refresh_ms and read everywhere else.
    alignas(64) std::atomic<uint32_t> seq_{ 0 };
    std::atomic<uint64_t> tsc0_{ 0 };
    std::atomic<uint64_t> ns0_{ 0 };
    std::atomic<uint64_t> mult_{ 0 };
    alignas(64) std::atomic<uint8_t> mode_{ 0 }; // 0: system clock, 1: calibrated TSC
    // Calibrator-only state.
    Model cur_{ 0, 0, 0 };
    double slope_{ 0.3 };
    std::deque<Sample> hist_;
    std::atomic<uint64_t> refreshes_{ 0 }, skipped_{ 0 }, steps_{ 0 };
    std::atomic<int64_t> last_error_{ 0 }, max_abs_error_{ 0 };
    std::atomic<bool> stop_{ false };
    std::thread thread_;
};

using Clock = ClockT<SystemSource>;

// The process-wide clock behind get_synced_time_ns(). Constructed during static initialization; reads taken
// before that finishes fall back to the system clock.
inline Clock g_clock;

} // namespace tscclock
