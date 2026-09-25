# main.cpp is excluded from the library (belongs to the standalone
# PerformanceServiceApp executable only) so linking PerformanceService
# elsewhere, e.g. from EngineTests, doesn't pull in a second main().
file(GLOB PERFORMANCE_SOURCES "*.cpp")
list(REMOVE_ITEM PERFORMANCE_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
add_library(PerformanceService STATIC ${PERFORMANCE_SOURCES})

# Expose this directory's headers to downstream applications
target_include_directories(PerformanceService PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# NetworkConfig.hpp (listen port default) comes from NetworkService;
# OrderTrace/TelemetryReport come from ExchangeCommon; percentile computation
# comes from the vendored HdrHistogram.
target_link_libraries(PerformanceService PRIVATE ExchangeCommon NetworkService hdr_histogram::hdr_histogram_static)

# Standalone process: one per deployment. Receives forwarded telemetry from
# every MatchingService replica and holds the one canonical view of "state
# at each replicator" for admin monitoring.
add_executable(PerformanceServiceApp main.cpp)
target_link_libraries(PerformanceServiceApp PRIVATE PerformanceService ExchangeCommon NetworkService hdr_histogram::hdr_histogram_static Threads::Threads)
