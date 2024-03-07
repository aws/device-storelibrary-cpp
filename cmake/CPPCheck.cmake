if(NOT CPPCHECK_EXECUTABLE)
    set(CPPCHECK_EXECUTABLE cppcheck)
endif()

if(NOT EXISTS ${CPPCHECK_EXECUTABLE})
    find_program(cppcheck_executable_tmp ${CPPCHECK_EXECUTABLE})
    if(cppcheck_executable_tmp)
        set(CPPCHECK_EXECUTABLE ${cppcheck_executable_tmp})
        unset(cppcheck_executable_tmp)
    else()
        message("cppcheck: ${CPPCHECK_EXECUTABLE} not found!")
        unset(CPPCHECK_EXECUTABLE)
    endif()
endif()

function(target_cppcheck_setup target_name)
    if(EXISTS ${CPPCHECK_EXECUTABLE})
        list(APPEND
                CPP_CHECK_OPTS
                ${CPPCHECK_EXECUTABLE}
                "--enable=warning,performance,portability,missingInclude"
                "--inconclusive"
                "--force"
                "--inline-suppr"
                "--suppress=missingIncludeSystem"
                "--suppressions-list=${CMAKE_SOURCE_DIR}/CppCheckSuppressions.txt"
                "--error-exitcode=1"
        )
        set_target_properties(${target_name} PROPERTIES CXX_CPPCHECK "${CPP_CHECK_OPTS}")
    endif()
endfunction()
