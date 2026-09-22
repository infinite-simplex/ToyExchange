cmake_minimum_required(VERSION 3.20)
project(HighPerformanceExchange CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -mtune=native -Wall -Wextra -pthread")
endif()

find_package(Threads REQUIRED)

# Subdirectories
add_subdirectory(ExchangeCommon)
add_subdirectory(MatchingService)
add_subdirectory(NetworkService)
add_subdirectory(Tools/OuchTestClient)

# Main Executable
add_executable(ExchangeMain main.cpp)
target_link_libraries(ExchangeMain 
    PRIVATE 
        NetworkService
        MatchingService
        ExchangeCommon
        Threads::Threads
)

# ==============================================================================
# Testing Setup (GoogleTest + CTest)
# ==============================================================================
enable_testing()

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)
# For Windows MSVC runtime compatibility (safe to include on Linux)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Add Test Executable
add_executable(EngineTests Testing.cpp)

target_link_libraries(EngineTests
    PRIVATE
        gtest_main
        MatchingService
        NetworkService
        ExchangeCommon
        Threads::Threads
)

include(GoogleTest)
gtest_discover_tests(EngineTests)