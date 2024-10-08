if(NOT CMAKEFORMAT_EXECUTABLE)
    set(CMAKEFORMAT_EXECUTABLE cmake-format)
endif()

if(NOT EXISTS ${CMAKEFORMAT_EXECUTABLE})
    find_program(cmakeformat_executable_tmp ${CMAKEFORMAT_EXECUTABLE})
    if(cmakeformat_executable_tmp)
        set(CMAKEFORMAT_EXECUTABLE ${cmakeformat_executable_tmp})
        unset(cmakeformat_executable_tmp)
    else()
        message("cmake-format: ${CMAKEFORMAT_EXECUTABLE} not found!")
        unset(CMAKEFORMAT_EXECUTABLE)
    endif()
endif()

if(EXISTS ${CMAKEFORMAT_EXECUTABLE})
    file(GLOB_RECURSE CMAKE_FILES "CMakeLists.txt" "*.cmake")
    set(EXCLUDE_DIR "/build/")
    foreach(TMP_PATH ${CMAKE_FILES})
        string(FIND ${TMP_PATH} ${EXCLUDE_DIR} EXCLUDE_DIR_FOUND)
        if(NOT ${EXCLUDE_DIR_FOUND} EQUAL -1)
            list(REMOVE_ITEM CMAKE_FILES ${TMP_PATH})
        endif()
    endforeach(TMP_PATH)

    add_custom_target(cmake-format COMMAND ${CMAKEFORMAT_EXECUTABLE} -i ${CMAKE_FILES})
endif()
