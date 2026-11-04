// Tests for tscclock::ClockT against a scripted clock: a virtual world where true time, the TSC and the "system
// clock" (what clock_gettime(CLOCK_TAI) returns) are independent, so slews, steps and TAI-offset changes can be
// injected exactly. No root and no real clock is touched, except in the last two tests.
#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <ctime>
#include <vector>

#include "Telemetry.hpp"
#include "TscClock.hpp"

namespace {

using tscclock::ClockT;
using tscclock::Config;

struct World {
    double t = 0;                       // true time, ns
    double tsc_hz = 3.8e9;              // the TSC's rate (constant, as on an invariant TSC)
    // System clock: tai(t) = tai_anchor + (t - t_anchor) * (1 + ppm*1e-6)
    int64_t tai_anchor = 1'700'000'000'000'000'000ll; // integer: a double cannot resolve nanoseconds at this magnitude
    double t_anchor = 0, sys_ppm = 0;
    bool invariant = true;

    int64_t tai_at(double tt) const { return tai_anchor + static_cast<int64_t>(std::llround((tt - t_anchor) * (1 + sys_ppm * 1e-6))); }
    void set_sys_ppm(double ppm) { tai_anchor = tai_at(t); t_anchor = t; sys_ppm = ppm; }
    void step_sys(double ns) { tai_anchor += static_cast<int64_t>(ns); }
    uint64_t ticks(double tt) const { return static_cast<uint64_t>(tt * tsc_hz / 1e9); }
};

struct FakeSource {
    World* w;
    uint64_t tsc() noexcept { return w->ticks(w->t); }
    uint64_t tsc_ordered() noexcept { return w->ticks(w->t); }
    int64_t tai_ns() noexcept { // a 20 ns call, read in the middle
        w->t += 10;
        const int64_t v = w->tai_at(w->t);
        w->t += 10;
        return v;
    }
    void sleep_ms(unsigned ms) { w->t += ms * 1e6; }
    bool tsc_usable() const { return w->invariant; }
};

struct DriveResult { double max_abs = 0; double p99 = 0; uint64_t backwards = 0; };

// Runs `seconds` of virtual time, refreshing every 10 ms and probing the error four times per interval at random
// phases. Returns the largest |error| seen (ns) from `from_s` on.
template <class F>
DriveResult drive(World& w, ClockT<FakeSource>& c, double seconds, double from_s, F&& script, uint64_t seed = 1) {
    constexpr double interval = 10e6;
    std::vector<double> errs;
    DriveResult r;
    uint64_t prev = 0;
    uint64_t rng = seed;
    const double end = w.t + seconds * 1e9, t0 = w.t;
    while (w.t < end) {
        script((w.t - t0) / 1e9);
        for (int i = 0; i < 4; ++i) {
            rng = rng * 6364136223846793005ull + 1442695040888963407ull;
            w.t += (rng >> 33) % static_cast<uint64_t>(interval / 5);
            const uint64_t got = c.now_ns();
            const double err = static_cast<double>(static_cast<int64_t>(got) - w.tai_at(w.t));
            if ((w.t - t0) / 1e9 >= from_s) errs.push_back(std::fabs(err));
            if (got < prev) ++r.backwards;
            prev = got;
        }
        w.t += interval / 5; // on to the next refresh boundary
        c.refresh();
    }
    std::sort(errs.begin(), errs.end());
    if (!errs.empty()) { r.max_abs = errs.back(); r.p99 = errs[static_cast<size_t>(0.99 * (errs.size() - 1))]; }
    return r;
}

} // namespace

TEST(TscClockTest, SteadySystemClockStaysWithinTenNanoseconds) {
    World w; FakeSource s{ &w }; ClockT<FakeSource> c(s, Config{}, false);
    const DriveResult r = drive(w, c, 30, 1, [](double) {});
    EXPECT_LT(r.max_abs, 10.0);
    EXPECT_EQ(0u, c.stats().steps);
}

TEST(TscClockTest, ASystemClockRateRampIsFollowedWithoutAStepOrAReadGoingBackwards) {
    World w; FakeSource s{ &w }; ClockT<FakeSource> c(s, Config{}, false);
    // 0 -> +50 ppm at 1 ppm/s: a servo converging
    const DriveResult r = drive(w, c, 60, 1, [&](double t) { w.set_sys_ppm(std::min(50.0, t * 1.0)); });
    EXPECT_LT(r.max_abs, 100.0);
    EXPECT_EQ(0u, r.backwards);
    EXPECT_EQ(0u, c.stats().steps) << "a slew must not look like a step";
}

TEST(TscClockTest, AnAbruptRateChangeDriftsOnlyUntilTheNextRefreshesSeeIt) {
    World w; FakeSource s{ &w }; ClockT<FakeSource> c(s, Config{}, false);
    bool done = false;
    // +100 ppm at once drifts 1 us per 10 ms until it is observed
    const DriveResult r = drive(w, c, 10, 0.5, [&](double t) { if (t > 3 && !done) { w.set_sys_ppm(100); done = true; } });
    EXPECT_LT(r.max_abs, 5'000.0);
    EXPECT_EQ(0u, c.stats().steps);
}

TEST(TscClockTest, StepsOfTheSystemClockAreCountedAndFollowed) {
    World w; FakeSource s{ &w }; ClockT<FakeSource> c(s, Config{}, false);
    int phase = 0;
    // +1 ms, then -2 ms, then the TAI offset is set to +37 s
    drive(w, c, 12, 0, [&](double t) {
        if (t > 2 && phase == 0) { w.step_sys(1e6); phase = 1; }
        if (t > 5 && phase == 1) { w.step_sys(-2e6); phase = 2; }
        if (t > 8 && phase == 2) { w.step_sys(37e9); phase = 3; }
    });
    EXPECT_EQ(3u, c.stats().steps);
    for (int i = 0; i < 3; ++i) { w.t += 10e6; c.refresh(); }
    const double err = static_cast<double>(static_cast<int64_t>(c.now_ns()) - w.tai_at(w.t));
    EXPECT_LT(std::fabs(err), 20.0) << "30 ms after the TAI-offset jump";
}

TEST(TscClockTest, ReadsNeverGoBackwardsWhileTheModelIsBeingSteered) {
    World w; FakeSource s{ &w }; ClockT<FakeSource> c(s, Config{}, false);
    uint64_t prev = 0, back = 0;
    for (int k = 0; k < 2000; ++k) { // the system clock runs 200 ppm slow, then fast
        w.set_sys_ppm((k / 200) % 2 ? 200.0 : -200.0);
        for (int i = 0; i < 100; ++i) { w.t += 100'000; const uint64_t g = c.now_ns(); back += g < prev; prev = g; }
        c.refresh();
    }
    EXPECT_EQ(0u, back);
}

TEST(TscClockTest, ACalibrationSampleTakenWhileTheCalibratorWasPreemptedIsDiscarded) {
    struct SlowSource : FakeSource { // every system-clock call takes 50 us: the calibrator was descheduled
        int64_t tai_ns() noexcept { w->t += 10; const int64_t v = w->tai_at(w->t); w->t += 10 + 50'000; return v; }
    };
    World w; SlowSource s{ { &w } }; ClockT<SlowSource> c(s, Config{}, false);
    const auto a = c.stats();
    w.t += 10e6; c.refresh();
    const auto b = c.stats();
    EXPECT_EQ(a.skipped_wide_bracket + 1, b.skipped_wide_bracket);
    EXPECT_EQ(a.refreshes, b.refreshes) << "a discarded sample must not count as a refresh";
}

TEST(TscClockTest, WithoutAnInvariantTscEveryReadIsTheSystemClock) {
    World w; w.invariant = false; FakeSource s{ &w }; ClockT<FakeSource> c(s, Config{}, false);
    EXPECT_FALSE(c.stats().using_tsc);
    w.t += 12345;
    const uint64_t got = c.now_ns();
    EXPECT_LE(std::llabs(static_cast<int64_t>(got) - w.tai_at(w.t)), 20);
}

TEST(TscClockTest, IfTheCalibratorStopsTheErrorGrowsWithTheRateUncertaintyNotWithoutBound) {
    World w; FakeSource s{ &w }; ClockT<FakeSource> c(s, Config{}, false);
    drive(w, c, 5, 0, [](double) {});
    w.set_sys_ppm(1.0); // the system clock drifts 1 ppm relative to the model's slope
    w.t += 1e9;         // one second with no refresh
    const double err = static_cast<double>(static_cast<int64_t>(c.now_ns()) - w.tai_at(w.t));
    EXPECT_LT(std::fabs(err), 1500.0) << "about 1 ppm x 1 s = 1000 ns";
}

// The two tests below use the real process-wide clock behind get_synced_time_ns().

TEST(TscClockTest, TheProcessClockAgreesWithTheSystemClock) {
    timespec ts;
    clock_gettime(CLOCK_TAI, &ts);
    const int64_t before = static_cast<int64_t>(ts.tv_sec) * 1'000'000'000ll + ts.tv_nsec;
    const int64_t got = static_cast<int64_t>(get_synced_time_ns());
    clock_gettime(CLOCK_TAI, &ts);
    const int64_t after = static_cast<int64_t>(ts.tv_sec) * 1'000'000'000ll + ts.tv_nsec;
    // Loose on purpose: a descheduled test thread can sit between the reads. The tight bound is the scripted tests.
    EXPECT_GE(got, before - 100'000);
    EXPECT_LE(got, after + 100'000);
}

TEST(TscClockTest, TheProcessClockNeverGoesBackwardsOnOneThread) {
    uint64_t prev = get_synced_time_ns();
    uint64_t back = 0;
    for (int i = 0; i < 2'000'000; ++i) {
        const uint64_t g = get_synced_time_ns();
        back += g < prev;
        prev = g;
    }
    EXPECT_EQ(0u, back);
}
