cmake_minimum_required(VERSION 3.25)
project(greenlake)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

include(cmake/ClangTidy.cmake)
include(cmake/CMakeFormat.cmake)

if(CMAKE_BUILD_TYPE MATCHES "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    message("IPO enabled, debugging will not work")
endif()

include(cmake/ClangFormat.cmake)

add_library(kv include/kv.hpp include/expected.hpp include/crc32.hpp include/filesystem.hpp include/slices.hpp
        include/util.hpp
        source/kv.cpp)
set_target_properties(kv PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(kv PRIVATE include)
target_compile_options(kv PRIVATE -fno-rtti -fno-exceptions -Wall -Wextra -Werror)
target_clangformat_setup(kv)

add_library(greenlake_stream include/stream.hpp include/fileStream.hpp
        include/memoryStream.hpp include/crc32.hpp include/slices.hpp
        source/memoryStream.cpp source/fileStream.cpp
        include/expected.hpp
        include/util.hpp)
set_target_properties(greenlake_stream PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(greenlake_stream PRIVATE include)
target_link_libraries(greenlake_stream PUBLIC kv)
target_compile_options(greenlake_stream PRIVATE -fno-rtti -Wall -Wextra -Werror)
target_clangformat_setup(greenlake_stream)

set(CMAKE_CXX_STANDARD 17)
add_executable(greenlake_example include/posixFileSystem.hpp source/main.cpp)
target_include_directories(greenlake_example PRIVATE include)
target_link_libraries(greenlake_example PRIVATE greenlake_stream)
target_compile_options(greenlake_example PRIVATE -fno-rtti -Wall -Wextra -Werror)
target_clangformat_setup(greenlake_example)

if(CMAKE_BUILD_TYPE MATCHES "Debug")
    include(cmake/Sanitizers.cmake)
    myproject_enable_sanitizers(
        greenlake_example
        ON # asan
        ON # ub
        OFF # thread
        OFF # memory
    )
    message("ASAN and UBSan enabled")
endif()

# Setup unit tests
enable_testing()
add_custom_target(build-tests COMMENT "Build all the unit tests.")
add_test(NAME build-tests
        COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --target build-tests)
add_subdirectory(test)

# Brazil targets after this point

# Setup a default target that runs when no target is specified.
add_custom_target(default DEPENDS greenlake_example)
if (TARGET cmakeformat)
    add_dependencies(default cmakeformat)
endif ()
if (TARGET clangformat)
    add_dependencies(default clangformat)
endif ()

# Setup the 'release' target (which is run on Package Builder).
add_custom_target(release
        COMMENT "Build and install the library, and run the unit tests."
        # TODO: execute tests
        # COMMAND "${CMAKE_COMMAND}" -E chdir "${CMAKE_BINARY_DIR}" "${CMAKE_CTEST_COMMAND}" --output-on-failure
        COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --target greenlake_example
        DEPENDS build-tests
        USES_TERMINAL)
