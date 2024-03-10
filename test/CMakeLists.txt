set(CMAKE_CXX_STANDARD 17)

# Find Catch2 if we can. If we cannot, then download it with CPM
find_package(Catch2 CONFIG QUIET)
if(NOT Catch2_FOUND)
    include(../cmake/CPM.cmake)
    cpmaddpackage("gh:catchorg/Catch2@3.5.2")
    find_package(Catch2 3 REQUIRED)
else()
    find_package(Catch2 3 REQUIRED)
endif()
# Path to module "Catch" containing catch_discover_tests()
list(APPEND CMAKE_MODULE_PATH ${Catch2_SOURCE_DIR}/extras)

# These tests can use the Catch2-provided main
add_executable(tests kv_test.cpp test_utils.cpp test_utils.hpp stream_test.cpp)
target_link_libraries(tests PRIVATE Catch2::Catch2WithMain greenlake_stream)
target_clangformat_setup(tests)
target_compile_options(tests PRIVATE ${STORE_COMPILE_FLAGS})

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
