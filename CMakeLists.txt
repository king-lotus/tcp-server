cmake_minimum_required(VERSION 3.16)
project(tcp-server-mt LANGUAGES CXX)

# verify/run.sh stays the gate: it is what CI runs, and it is the only place
# the three configurations (plain, asan+ubsan, tsan), cpplint and the suite
# ordering are written down. This file exists so the same sources can be built
# by a build system, opened in an IDE, and run one suite at a time through
# ctest -- not as a second, drifting definition of what "verified" means.

add_library(tcp_server_mt INTERFACE)
target_include_directories(tcp_server_mt INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(tcp_server_mt INTERFACE cxx_std_17)
find_package(Threads REQUIRED)
target_link_libraries(tcp_server_mt INTERFACE Threads::Threads)

option(TCP_BUILD_SUITES "build the verify/ suites" ON)
option(TCP_BUILD_BENCH  "build the verify/ benchmarks" OFF)
set(TCP_SANITIZE "" CACHE STRING
    "sanitizers for the suites: address,undefined | thread | empty")

if(NOT MSVC)
    add_compile_options(-Wall -Wextra -Wpedantic -Wshadow)
endif()

if(TCP_SANITIZE)
    # ASan and TSan cannot share a binary, which is why the shell runner builds
    # one configuration at a time and so does this option.
    add_compile_options(-g -fsanitize=${TCP_SANITIZE} -fno-omit-frame-pointer)
    add_link_options(-fsanitize=${TCP_SANITIZE})
endif()

if(TCP_BUILD_SUITES)
    enable_testing()

    set(TCP_SUITES
        close_semantics
        idle_semantics
        ping_pong
        send_results
        backpressure
        reactor_spread
        config_budget
        harness_selftest)

    foreach(suite IN LISTS TCP_SUITES)
        add_executable(${suite} verify/${suite}.cpp)
        target_link_libraries(${suite} PRIVATE tcp_server_mt)
    endforeach()

    # fault_injection #errors without the macro on purpose: a suite that
    # quietly compiles away is worse than one that refuses to build.
    add_executable(fault_injection verify/fault_injection.cpp)
    target_link_libraries(fault_injection PRIVATE tcp_server_mt)
    target_compile_definitions(fault_injection PRIVATE INDUSTRIAL_TCP_TESTING)

    # TSan reserves a fixed shadow region and dies on kernels that hand out
    # high vm.mmap_rnd_bits. verify/run.sh wraps those runs in `setarch -R`,
    # and ctest needs the same wrap or every tsan test fails before main().
    set(TCP_TEST_PREFIX)
    if(TCP_SANITIZE MATCHES "thread")
        find_program(TCP_SETARCH setarch)
        if(TCP_SETARCH)
            list(APPEND TCP_TEST_PREFIX ${TCP_SETARCH} ${CMAKE_SYSTEM_PROCESSOR} -R)
        endif()
    endif()

    # The same ten runs run.sh performs: seven suites, plus the two that run a
    # second time at a different reactor count.
    add_test(NAME close_semantics
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:close_semantics>)
    add_test(NAME close_semantics_1_reactor
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:close_semantics>)
    set_tests_properties(close_semantics_1_reactor PROPERTIES
                         ENVIRONMENT "CS_REACTORS=1")
    add_test(NAME idle_semantics
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:idle_semantics>)
    add_test(NAME ping_pong
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:ping_pong>)
    add_test(NAME send_results
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:send_results>)
    add_test(NAME backpressure
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:backpressure>)
    add_test(NAME fault_injection
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:fault_injection>)
    add_test(NAME reactor_spread
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:reactor_spread>)
    add_test(NAME reactor_spread_2_reactors
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:reactor_spread>)
    set_tests_properties(reactor_spread_2_reactors PROPERTIES
                         ENVIRONMENT "RS_REACTORS=2")
    add_test(NAME config_budget
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:config_budget>)
    add_test(NAME harness_selftest
             COMMAND ${TCP_TEST_PREFIX} $<TARGET_FILE:harness_selftest>)

    # run.sh gives each case 180s so a hang presents as a failure; the same
    # bound here keeps ctest from outliving the CI job.
    set_tests_properties(close_semantics close_semantics_1_reactor
                         idle_semantics ping_pong send_results backpressure
                         fault_injection reactor_spread
                         reactor_spread_2_reactors config_budget
                         harness_selftest
                         PROPERTIES TIMEOUT 180)
endif()

if(TCP_BUILD_BENCH)
    foreach(bench bench_rss bench_rx bench_send bench_floor)
        add_executable(${bench} verify/${bench}.cpp)
        target_link_libraries(${bench} PRIVATE tcp_server_mt)
        if(NOT MSVC)
            target_compile_options(${bench} PRIVATE -O2)
        endif()
    endforeach()
endif()
