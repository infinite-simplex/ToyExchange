# main.cpp is excluded from the library (belongs to the standalone
# MatchingServiceApp executable only) so linking MatchingService elsewhere,
# e.g. from EngineTests, doesn't pull in a second main().
file(GLOB MATCHING_SOURCES "*.cpp")
list(REMOVE_ITEM MATCHING_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")
add_library(MatchingService STATIC ${MATCHING_SOURCES})

# Expose this directory's headers to downstream applications
target_include_directories(MatchingService PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Automatically pull in ExchangeCommon headers when someone requests MatchingService
target_link_libraries(MatchingService PRIVATE ExchangeCommon)

# Standalone process: one per replica. Joins NetworkServiceApp's replication
# multicast feed and runs the matching engine against it, so replicas on
# different machines can be started/stopped independently for fault tolerance.
add_executable(MatchingServiceApp main.cpp)
target_link_libraries(MatchingServiceApp PRIVATE MatchingService NetworkService ExchangeCommon Threads::Threads)
