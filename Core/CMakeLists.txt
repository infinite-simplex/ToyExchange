# The reusable, product-agnostic exchange pipeline: ingress (sequencing,
# durable log, gap-fill retransmit), egress (fenced fan-out), replica leader
# arbitration, the generic replica host (MatchingService<TCommand,TEngine>),
# and the shared low-level types (SPSCQueue, telemetry) everything above is
# built on. Nothing in this directory names an event-contract/OUCH type —
# see ExchangeCommon/ and NetworkService/OUCH_ITCH/ for that product's own
# code, built on top of this.
#
# Header-only (every file here is a template or has its methods defined
# inline in the class body), so this is an INTERFACE library, same idiom as
# ExchangeCommon's own CMakeLists.txt.
add_library(Core INTERFACE)

# Flat #include "X.hpp" regardless of subfolder, same convention already
# used by NetworkService's own CMakeLists.txt — keeps the folder split
# purely organizational, with no include-statement churn anywhere else.
target_include_directories(Core INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/Ingress
    ${CMAKE_CURRENT_SOURCE_DIR}/Egress
    ${CMAKE_CURRENT_SOURCE_DIR}/Arbitration
)

target_link_libraries(Core INTERFACE Threads::Threads)
