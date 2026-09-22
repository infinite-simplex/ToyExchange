add_library(ExchangeCommon INTERFACE)

# Expose this directory's headers to anything that links against it
target_include_directories(ExchangeCommon INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
