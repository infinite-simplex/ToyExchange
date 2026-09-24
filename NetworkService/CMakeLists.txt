# Gather all .cpp source files inside NetworkService and its subfolders
# (Ingress/, Egress/, OUCH_ITCH/, GPU_PROT/), excluding main.cpp which
# belongs to the standalone NetworkServiceApp executable only (kept out of
# the library so linking it, e.g. from EngineTests, doesn't pull in a
# second main()).
file(GLOB_RECURSE NETWORK_SOURCES "*.cpp")
list(REMOVE_ITEM NETWORK_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/main.cpp")

# Define the static binary target
add_library(NetworkService STATIC ${NETWORK_SOURCES})

# Every header still uses a flat #include "X.hpp" regardless of which
# subfolder it now lives in, so each subfolder needs to be on the include
# path too — this keeps the folder split purely organizational, with no
# include-statement churn anywhere else in the codebase.
target_include_directories(NetworkService PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/Ingress
    ${CMAKE_CURRENT_SOURCE_DIR}/Egress
    ${CMAKE_CURRENT_SOURCE_DIR}/OUCH_ITCH
    ${CMAKE_CURRENT_SOURCE_DIR}/GPU_PROT
    ${CMAKE_CURRENT_SOURCE_DIR}/Arbitration
)

# Private linkage means it consumes ExchangeCommon internally
target_link_libraries(NetworkService PRIVATE ExchangeCommon Threads::Threads)

# Standalone process: order-entry ingress + sequencing + multicast
# replication fan-out + gap-fill retransmit server. Deploy one per exchange
# instance; MatchingService replicas subscribe to its multicast feed.
add_executable(NetworkServiceApp main.cpp)
target_link_libraries(NetworkServiceApp PRIVATE NetworkService ExchangeCommon Threads::Threads)
