cmake_minimum_required(VERSION 3.10.2)
project(greenlake)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

include(cmake/ClangTidy.cmake)
include(cmake/CMakeFormat.cmake)

option(STORE_USE_LTO "Build using LTO in release mode" ON)
if(CMAKE_BUILD_TYPE MATCHES "Release" AND STORE_USE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    message("IPO enabled, debugging will not work")
endif()

include(cmake/ClangFormat.cmake)

add_library(
    greenlake_kv
    include/aws/store/kv/kv.hpp
    include/aws/store/common/expected.hpp
    include/aws/store/common/crc32.hpp
    include/aws/store/filesystem/filesystem.hpp
    include/aws/store/common/slices.hpp
    include/aws/store/common/util.hpp
    src/kv/kv.cpp
    include/aws/store/common/logging.hpp
)
set_target_properties(greenlake_kv PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(greenlake_kv PUBLIC include)
target_compile_options(greenlake_kv PRIVATE -fno-exceptions -Wall -Wextra -Werror -Wconversion -pedantic)
if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
    # Keep RTTI in debug for UBSan
    target_compile_options(greenlake_kv PRIVATE -fno-rtti)
endif()
target_clangformat_setup(greenlake_kv)
target_clangtidy_setup(greenlake_kv)

add_library(
    greenlake_stream
    include/aws/store/stream/stream.hpp
    include/aws/store/stream/fileStream.hpp
    include/aws/store/stream/memoryStream.hpp
    include/aws/store/common/crc32.hpp
    include/aws/store/common/slices.hpp
    src/stream/memoryStream.cpp
    src/stream/fileStream.cpp
    include/aws/store/common/expected.hpp
    include/aws/store/common/util.hpp
    include/aws/store/common/logging.hpp
    src/stream/fileSegment.cpp
    src/stream/stream.cpp
)
set_target_properties(greenlake_stream PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(greenlake_stream PUBLIC greenlake_kv)
target_compile_options(greenlake_stream PRIVATE -fno-exceptions -Wall -Wextra -Werror -Wconversion -pedantic)
if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
    # Keep RTTI in debug for UBSan
    target_compile_options(greenlake_stream PRIVATE -fno-rtti)
endif()
target_clangformat_setup(greenlake_stream)
target_clangtidy_setup(greenlake_stream)

include(GNUInstallDirs)
install(TARGETS greenlake_kv greenlake_stream LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/aws DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

option(STORE_BUILD_EXAMPLE "Build example program" ON)
if(STORE_BUILD_EXAMPLE)
    set(CMAKE_CXX_STANDARD 17)
    add_executable(greenlake_example include/aws/store/filesystem/posixFileSystem.hpp src/main.cpp)
    target_link_libraries(greenlake_example PRIVATE greenlake_stream)
    target_compile_options(greenlake_example PRIVATE -fno-exceptions -Wall -Wextra -Werror -Wconversion -pedantic)
    if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
        # Keep RTTI in debug for UBSan
        target_compile_options(greenlake_example PRIVATE -fno-rtti)
    endif()
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

    # Setup a default target that runs when no target is specified.
    add_custom_target(default DEPENDS greenlake_example)
    if(TARGET cmakeformat)
        add_dependencies(default cmakeformat)
    endif()
endif()

option(STORE_BUILD_TESTS "Build tests" ON)
if(STORE_BUILD_TESTS)
    # Setup unit tests
    enable_testing()
    add_subdirectory(test)

    list(JOIN CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES : DISCOVERED_LD_PATH)
    # Setup the 'release' target (which is run on Package Builder).
    add_custom_target(
        release
        COMMENT "Build and install the library, and run the unit tests."
        COMMAND "${CMAKE_COMMAND}" -E env LD_LIBRARY_PATH=${DISCOVERED_LD_PATH} "test/tests" --durations yes
        DEPENDS tests
        USES_TERMINAL
    )
endif()
