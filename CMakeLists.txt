# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.10.2)
project(device-storelibrary-cpp)

option(STORE_UNITY_BUILD "Build store using a unity build" ON)
if(STORE_UNITY_BUILD)
    set(CMAKE_UNITY_BUILD ON)
    set(CMAKE_UNITY_BUILD_MODE BATCH)
endif()

include(cmake/ClangTidy.cmake)
include(cmake/CPPCheck.cmake)
include(cmake/CMakeFormat.cmake)

option(STORE_USE_LLD "Link with lld on clang" ON)
option(STORE_USE_LTO "Build using LTO in release mode" ON)
if(CMAKE_BUILD_TYPE MATCHES "Release" AND STORE_USE_LTO)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    message("IPO enabled, debugging will not work")
endif()

# TODO: MSVC is not supported right now. Build only works without error on Linux
if(MSVC)
    # /wd4820 ignore extra padding added to structs
    # /wd4626 ignore implicitly deleted assignment operator
    # /wd5027 ignore implicitly deleted move assignment operator
    list(APPEND STORE_COMPILE_FLAGS "/EHs-c-" "/D_HAS_EXCEPTIONS=0" "/Wall" "/WX" "/wd4820" "/wd4626" "/wd5027")
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
    if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND STORE_USE_LLD)
        list(APPEND STORE_LINK_FLAGS "-fuse-ld=lld")
    endif()
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

include(GNUInstallDirs)
set_target_properties(kv PROPERTIES CXX_STANDARD 11 CXX_VISIBILITY_PRESET hidden)
target_include_directories(
    kv PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>" "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
)
target_compile_options(kv PRIVATE ${STORE_COMPILE_FLAGS})
target_link_options(kv PRIVATE ${STORE_LINK_FLAGS})
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
set_target_properties(stream PROPERTIES CXX_STANDARD 11 CXX_VISIBILITY_PRESET hidden)
target_link_libraries(stream PUBLIC kv)
target_compile_options(stream PRIVATE ${STORE_COMPILE_FLAGS})
target_link_options(stream PRIVATE ${STORE_LINK_FLAGS})
target_clangformat_setup(stream)
target_clangtidy_setup(stream)
target_cppcheck_setup(stream)

install(
    TARGETS stream
    EXPORT lib-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(
    TARGETS kv
    EXPORT lib-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
)
install(DIRECTORY include/aws DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(
    EXPORT lib-targets
    NAMESPACE aws::store::
    FILE aws-store-targets.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/aws
)

include(CMakePackageConfigHelpers)
configure_package_config_file(
    cmake/aws-storeConfig.cmake.in ${CMAKE_CURRENT_BINARY_DIR}/aws-storeConfig.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/aws
)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/aws-storeConfig.cmake DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/aws)

option(STORE_BUILD_EXAMPLE "Build example program" ON)
if(STORE_BUILD_EXAMPLE)
    add_executable(example include/aws/store/filesystem/posixFileSystem.hpp src/main.cpp)
    set_target_properties(example PROPERTIES CXX_STANDARD 17)
    target_link_libraries(example PRIVATE stream)
    target_compile_options(example PRIVATE ${STORE_COMPILE_FLAGS})
    target_link_options(example PRIVATE ${STORE_LINK_FLAGS})
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
