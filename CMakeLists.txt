cmake_minimum_required(VERSION 3.10.2)
project(aws::store)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

include(cmake/ClangTidy.cmake)
include(cmake/CPPCheck.cmake)
include(cmake/CMakeFormat.cmake)

option(STORE_USE_LTO "Build using LTO in release mode" ON)
if(CMAKE_BUILD_TYPE MATCHES "Release" AND STORE_USE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    message("IPO enabled, debugging will not work")
endif()

if(MSVC)
    list(APPEND STORE_COMPILE_FLAGS "/EHs-c-" "/D_HAS_EXCEPTIONS=0" "/Wall" "/WX")
    if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
        # Keep RTTI in debug for UBSan
        list(APPEND STORE_COMPILE_FLAGS "/GR-")
    endif()
else()
    list(
        APPEND
        STORE_COMPILE_FLAGS
        "-fno-exceptions"
        "-Wall"
        "-Wextra"
        "-Werror"
        "-Wconversion"
        "-pedantic"
    )
    if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
        # Keep RTTI in debug for UBSan
        list(APPEND STORE_COMPILE_FLAGS "-fno-rtti")
    endif()
endif()

include(cmake/ClangFormat.cmake)

add_library(
    kv
    include/aws/store/kv/kv.hpp
    include/aws/store/common/expected.hpp
    include/aws/store/common/crc32.hpp
    include/aws/store/filesystem/filesystem.hpp
    include/aws/store/common/slices.hpp
    include/aws/store/common/util.hpp
    src/kv/kv.cpp
    include/aws/store/common/logging.hpp
)
add_library(${PROJECT_NAME}::kv ALIAS kv)
target_include_directories(kv PUBLIC include)
target_compile_options(kv PRIVATE ${STORE_COMPILE_FLAGS})
target_clangformat_setup(kv)
target_clangtidy_setup(kv)
target_cppcheck_setup(kv)

add_library(
    stream
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
add_library(${PROJECT_NAME}::stream ALIAS stream)
target_link_libraries(stream PUBLIC kv)
target_compile_options(stream PRIVATE ${STORE_COMPILE_FLAGS})
target_clangformat_setup(stream)
target_clangtidy_setup(stream)
target_cppcheck_setup(stream)

include(GNUInstallDirs)
install(TARGETS kv stream LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(DIRECTORY include/aws DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

option(STORE_BUILD_EXAMPLE "Build example program" ON)
if(STORE_BUILD_EXAMPLE)
    set(CMAKE_CXX_STANDARD 17)
    add_executable(example include/aws/store/filesystem/posixFileSystem.hpp src/main.cpp)
    target_link_libraries(example PRIVATE stream)
    target_compile_options(example PRIVATE ${STORE_COMPILE_FLAGS})
    target_clangformat_setup(example)
    target_clangtidy_setup(example)
    target_cppcheck_setup(example)

    if(CMAKE_BUILD_TYPE MATCHES "Debug")
        include(cmake/Sanitizers.cmake)
        myproject_enable_sanitizers(
            example
            ON # asan
            ON # ub
            OFF # thread
            OFF # memory
        )
        message("ASAN and UBSan enabled")
    endif()

    # Setup a default target that runs when no target is specified.
    add_custom_target(default DEPENDS example)
else()
    # Setup a default target that runs when no target is specified.
    add_custom_target(default DEPENDS stream)
endif()
if(TARGET cmake-format)
    add_dependencies(default cmake-format)
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
