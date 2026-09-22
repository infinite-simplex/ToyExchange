#pragma once
#include <chrono>

// "EST" here means a fixed UTC-5 offset, not DST-aware US-Eastern time:
// the build toolchain (GCC 11) has C++20 <chrono> calendar types (year_month_day,
// floor<days>, ...) but not the IANA timezone database (zoned_time/current_zone),
// which didn't land in libstdc++ until much later. Off by 1 hour during EDT
// (roughly mid-March - early November).
inline std::chrono::system_clock::time_point today_4pm_est_utc() {
    using namespace std::chrono;
    auto today = floor<days>(system_clock::now());
    return today + hours(16) + hours(5); // 16:00 EST == 21:00 UTC
}
