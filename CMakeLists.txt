cmake_minimum_required(VERSION 3.20)
project(HighPerformanceExchange CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -mtune=native -Wall -Wextra -pthread")
endif()

find_package(Threads REQUIRED)

include(FetchContent)

# HdrHistogram_c: vendored rather than hand-rolled — histogram/percentile
# computation (log-scale bucket sizing, O(1) recording/querying regardless of
# sample count) is a solved problem with real edge cases, not core
# learning-value territory for this project. Disable its log-file feature
# (needs zlib, which nothing else here depends on), its own tests/examples,
# and its shared-library variant (this repo only ever builds STATIC libs).
# Must resolve before add_subdirectory(PerformanceService) below, since that
# directory links the real static target, not just EngineTests.
FetchContent_Declare(
  hdr_histogram
  URL https://github.com/HdrHistogram/HdrHistogram_c/archive/refs/tags/0.11.10.zip
)
set(HDR_LOG_REQUIRED "DISABLED" CACHE STRING "" FORCE)
set(HDR_HISTOGRAM_BUILD_PROGRAMS OFF CACHE BOOL "" FORCE)
set(HDR_HISTOGRAM_BUILD_SHARED OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(hdr_histogram)

# Subdirectories — Core first: it's the reusable exchange pipeline every
# product (ExchangeCommon/MatchingService/NetworkService today) builds on.
add_subdirectory(Core)
add_subdirectory(ExchangeCommon)
add_subdirectory(MatchingService)
add_subdirectory(NetworkService)
add_subdirectory(PerformanceService)
add_subdirectory(Tools/OuchTestClient)

# Main Executable
add_executable(ExchangeMain main.cpp)
target_link_libraries(ExchangeMain
    PRIVATE
        NetworkService
        MatchingService
        ExchangeCommon
        Core
        Threads::Threads
)

# ==============================================================================
# Testing Setup (GoogleTest + CTest)
# ==============================================================================
enable_testing()

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# For Windows MSVC runtime compatibility (safe to include on Linux)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Two test executables, split along the same line the repo itself will
# eventually split on: CoreTests touches nothing product-specific, so it can
# move with Core; ProductTests exercises the OUCH product and stays behind.
add_executable(CoreTests Testing.cpp)
target_link_libraries(CoreTests
    PRIVATE
        gtest_main
        PerformanceService
        Core
        hdr_histogram::hdr_histogram_static
        Threads::Threads
)

add_executable(ProductTests ProductTesting.cpp)
target_link_libraries(ProductTests
    PRIVATE
        gtest_main
        MatchingService
        NetworkService
        ExchangeCommon
        Core
        Threads::Threads
)

include(GoogleTest)
gtest_discover_tests(CoreTests)
gtest_discover_tests(ProductTests)