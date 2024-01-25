cmake_minimum_required(VERSION 3.25)
project(greenlake_db)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

include(cmake/ClangTidy.cmake)
include(cmake/CMakeFormat.cmake)

if(CMAKE_BUILD_TYPE MATCHES "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    message("IPO enabled, debugging will not work")
endif()

include(cmake/ClangFormat.cmake)

add_library(greenlake_lib include/db.hpp include/fileDb.hpp
        include/memoryDb.hpp include/crc32.hpp
        source/memoryDb.cpp source/fileDb.cpp)
set_target_properties(greenlake_lib PROPERTIES LINKER_LANGUAGE CXX)
target_include_directories(greenlake_lib PRIVATE include)
target_clangformat_setup(greenlake_lib)

set(CMAKE_CXX_STANDARD 17)
add_executable(greenlake_db include/posixFileSystem.hpp source/main.cpp)
target_include_directories(greenlake_db PRIVATE include)
target_link_libraries(greenlake_db PRIVATE greenlake_lib)
target_clangformat_setup(greenlake_db)

if(CMAKE_BUILD_TYPE MATCHES "Debug")
    include(cmake/Sanitizers.cmake)
    myproject_enable_sanitizers(
        greenlake_db
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
add_custom_target(default DEPENDS greenlake_db)
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
        COMMAND "${CMAKE_COMMAND}" --build "${CMAKE_BINARY_DIR}" --target greenlake_db
        DEPENDS build-tests
        USES_TERMINAL)
