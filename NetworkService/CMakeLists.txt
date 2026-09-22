# Gather all .cpp source files inside the NetworkService folder, excluding
# main.cpp which belongs to the standalone NetworkServiceApp executable only
# (kept out of the library so linking it, e.g. from EngineTests, doesn't pull
# in a second main()).
file(GLOB NETWORK_SOURCES "*.cpp")
list(REMOVE_ITEM NETWORK_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")

# Define the static binary target
add_library(NetworkService STATIC ${NETWORK_SOURCES})

# Expose its public include path
target_include_directories(NetworkService PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

# Private linkage means it consumes ExchangeCommon internally
target_link_libraries(NetworkService PRIVATE ExchangeCommon Threads::Threads)

# Standalone process: order-entry ingress + sequencing + multicast
# replication fan-out + gap-fill retransmit server. Deploy one per exchange
# instance; MatchingService replicas subscribe to its multicast feed.
add_executable(NetworkServiceApp main.cpp)
target_link_libraries(NetworkServiceApp PRIVATE NetworkService ExchangeCommon Threads::Threads)
