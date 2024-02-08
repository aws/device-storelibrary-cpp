include(../cmake/CPM.cmake)

CPMAddPackage("gh:catchorg/Catch2@3.5.2")

find_package(Catch2 3 REQUIRED)
# These tests can use the Catch2-provided main
add_executable(tests test.cpp)
target_link_libraries(tests PRIVATE Catch2::Catch2WithMain greenlake_stream)
