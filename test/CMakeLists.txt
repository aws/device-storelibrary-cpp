include(../cmake/CPM.cmake)

CPMAddPackage("gh:catchorg/Catch2@3.5.2")

find_package(Catch2 3 REQUIRED)
# These tests can use the Catch2-provided main
add_executable(tests kv_test.cpp
        test_utils.hpp
        stream_test.cpp)
target_link_libraries(tests PRIVATE Catch2::Catch2WithMain greenlake_stream)
target_clangformat_setup(tests)

if(CMAKE_BUILD_TYPE MATCHES "Debug")
    include(../cmake/Sanitizers.cmake)
    myproject_enable_sanitizers(
            tests
            ON # asan
            ON # ub
            OFF # thread
            OFF # memory
    )
    message("ASAN and UBSan enabled")
endif()
