# Event-contract-specific shared vocabulary (order/event structs, GTD
# scheduler interface) — the generic pipeline types this used to share a
# directory with now live in Core/. Headers here #include "Alias.hpp" etc.,
# so this links Core rather than just exposing its own directory.
add_library(ExchangeCommon INTERFACE)

# Expose this directory's headers to anything that links against it
target_include_directories(ExchangeCommon INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(ExchangeCommon INTERFACE Core)
