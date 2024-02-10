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

add_library(greenlake_kv include/kv/kv.hpp include/common/expected.hpp include/common/crc32.hpp
        include/filesystem/filesystem.hpp include/common/slices.hpp
        include/common/util.hpp source/kv/kv.cpp
        include/common/logging.hpp)
set_target_properties(greenlake_kv PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(greenlake_kv PUBLIC include)
target_compile_options(greenlake_kv PRIVATE -fno-rtti -fno-exceptions -Wall -Wextra -Werror)
target_clangformat_setup(greenlake_kv)
target_clangtidy_setup(greenlake_kv)

add_library(greenlake_stream include/stream/stream.hpp include/stream/fileStream.hpp
        include/stream/memoryStream.hpp include/common/crc32.hpp include/common/slices.hpp
        source/stream/memoryStream.cpp source/stream/fileStream.cpp
        include/common/expected.hpp
        include/common/util.hpp
        include/common/logging.hpp
        source/stream/fileSegment.cpp)
set_target_properties(greenlake_stream PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(greenlake_stream PUBLIC greenlake_kv)
target_compile_options(greenlake_stream PRIVATE -fno-exceptions -fno-rtti -Wall -Wextra -Werror)
target_clangformat_setup(greenlake_stream)
target_clangtidy_setup(greenlake_stream)

set(CMAKE_CXX_STANDARD 17)
add_executable(greenlake_example include/filesystem/posixFileSystem.hpp source/main.cpp)
target_link_libraries(greenlake_example PRIVATE greenlake_stream)
target_compile_options(greenlake_example PRIVATE -fno-exceptions -fno-rtti -Wall -Wextra -Werror)
target_clangformat_setup(greenlake_example)
target_clangtidy_setup(greenlake_example)

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
add_subdirectory(test)

# Brazil targets after this point

# Setup a default target that runs when no target is specified.
add_custom_target(default DEPENDS greenlake_example)
if (TARGET cmakeformat)
    add_dependencies(default cmakeformat)
endif ()

# Setup the 'release' target (which is run on Package Builder).
add_custom_target(release
        COMMENT "Build and install the library, and run the unit tests."
        COMMAND "${CMAKE_COMMAND}" -E chdir "${CMAKE_BINARY_DIR}/test" "./tests" * --durations yes
        DEPENDS tests
        USES_TERMINAL)
