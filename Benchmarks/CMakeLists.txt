# Benchmark tools. Not part of the default build: configure the repo with
# -DTOYEXCHANGE_BENCHMARKS=ON. See README.md in this directory.
add_executable(IngressBench IngressBench.cpp)
target_include_directories(IngressBench PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(IngressBench
    PRIVATE
        Core
        hdr_histogram::hdr_histogram_static
        Threads::Threads
)

# Optional: the Aeron transports (aeron-ipc, aeron-udp). Point these at an Aeron
# checkout and its build tree (the C driver and client built with CMake 3.30 or later).
# The Aeron media driver, aeronmd, runs as a separate process.
set(TOYEXCHANGE_AERON_SRC "" CACHE PATH "Aeron source tree")
set(TOYEXCHANGE_AERON_BUILD "" CACHE PATH "Aeron build tree (contains lib/libaeron_static.a)")
if(TOYEXCHANGE_AERON_SRC AND TOYEXCHANGE_AERON_BUILD)
    target_compile_definitions(IngressBench PRIVATE TOYEXCHANGE_AERON)
    target_include_directories(IngressBench SYSTEM PRIVATE
        ${TOYEXCHANGE_AERON_SRC}/aeron-client/src/main/cpp_wrapper
        ${TOYEXCHANGE_AERON_SRC}/aeron-client/src/main/c)
    target_link_libraries(IngressBench PRIVATE
        ${TOYEXCHANGE_AERON_BUILD}/lib/libaeron_static.a
        ${CMAKE_DL_LIBS}
        m)
endif()
