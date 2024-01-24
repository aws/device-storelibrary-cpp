cmake_minimum_required(VERSION 3.25)
project(greenlake_db)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)

if(NOT CLANGTIDY_EXECUTABLE)
    set(CLANGTIDY_EXECUTABLE clang-tidy)
endif()

if(NOT EXISTS ${CLANGTIDY_EXECUTABLE})
    find_program(clangtidy_executable_tmp ${CLANGTIDY_EXECUTABLE})
    if(clangtidy_executable_tmp)
        set(CLANGTIDY_EXECUTABLE ${clangtidy_executable_tmp})
        unset(clangtidy_executable_tmp)
    else()
        message("Clang-Tidy: ${CLANGTIDY_EXECUTABLE} not found!")
        unset(CLANGTIDY_EXECUTABLE)
    endif()
endif()

if (EXISTS ${CLANGTIDY_EXECUTABLE})
    set(CMAKE_CXX_CLANG_TIDY ${CLANGTIDY_EXECUTABLE})
endif ()

if (CMAKE_BUILD_TYPE MATCHES "Release")
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    message("IPO enabled, debugging will not work")
endif ()

add_executable(greenlake_db
        source/memoryDb.cpp source/fileDb.cpp source/main.cpp
)
target_include_directories(greenlake_db PRIVATE include)

include(cmake/ClangFormat.cmake)
target_clangformat_setup(greenlake_db)

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    include(cmake/Sanitizers.cmake)
    myproject_enable_sanitizers(
            greenlake_db
            ON # asan
            ON # ub
            OFF # thread
            OFF # memory
    )
    message("ASAN and UBSan enabled")
endif ()

enable_testing()

# Brazil
add_custom_target(release ALL)
