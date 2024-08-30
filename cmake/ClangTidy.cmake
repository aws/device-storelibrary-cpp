# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

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

function(target_clangtidy_setup target_name)
    if(EXISTS ${CLANGTIDY_EXECUTABLE})
        set_target_properties(${target_name} PROPERTIES CXX_CLANG_TIDY ${CLANGTIDY_EXECUTABLE})
    endif()
endfunction()
