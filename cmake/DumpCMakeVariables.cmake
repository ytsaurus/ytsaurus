# Taken from http://www.cmake.org/Wiki/CMake_Useful_Variables/Logging_Useful_Variables

function(dump_cmake_variables)
    message(STATUS "CMAKE_BINARY_DIR:         " ${CMAKE_BINARY_DIR})
    message(STATUS "CMAKE_CURRENT_BINARY_DIR: " ${CMAKE_CURRENT_BINARY_DIR})

    message(STATUS "CMAKE_SOURCE_DIR:         " ${CMAKE_SOURCE_DIR})
    message(STATUS "CMAKE_CURRENT_SOURCE_DIR: " ${CMAKE_CURRENT_SOURCE_DIR})

    message(STATUS "PROJECT_BINARY_DIR:       " ${PROJECT_BINARY_DIR})
    message(STATUS "PROJECT_SOURCE_DIR:       " ${PROJECT_SOURCE_DIR})

    message(STATUS "EXECUTABLE_OUTPUT_PATH:   " ${EXECUTABLE_OUTPUT_PATH})
    message(STATUS "LIBRARY_OUTPUT_PATH:      " ${LIBRARY_OUTPUT_PATH})

    message(STATUS "CMAKE_ROOT:               " ${CMAKE_ROOT})
    message(STATUS "CMAKE_COMMAND:            " ${CMAKE_COMMAND})
    message(STATUS "CMAKE_MODULE_PATH:        " ${CMAKE_MODULE_PATH})

    message(STATUS "CMAKE_CURRENT_LIST_FILE:  " ${CMAKE_CURRENT_LIST_FILE})
    message(STATUS "CMAKE_CURRENT_LIST_LINE:  " ${CMAKE_CURRENT_LIST_LINE})

    message(STATUS "CMAKE_INCLUDE_PATH:       " ${CMAKE_INCLUDE_PATH})
    message(STATUS "CMAKE_LIBRARY_PATH:       " ${CMAKE_LIBRARY_PATH})

    message(STATUS "CMAKE_SYSTEM:             " ${CMAKE_SYSTEM})
    message(STATUS "CMAKE_SYSTEM_NAME:        " ${CMAKE_SYSTEM_NAME})
    message(STATUS "CMAKE_SYSTEM_VERSION:     " ${CMAKE_SYSTEM_VERSION})
    message(STATUS "CMAKE_SYSTEM_PROCESSOR:   " ${CMAKE_SYSTEM_PROCESSOR})

    message(STATUS "UNIX:                     " ${UNIX})
    message(STATUS "WIN32:                    " ${WIN32})
    message(STATUS "APPLE:                    " ${APPLE})
    message(STATUS "MINGW:                    " ${MINGW})
    message(STATUS "CYGWIN:                   " ${CYGWIN})
    message(STATUS "BORLAND:                  " ${BORLAND})
    message(STATUS "MSVC:                     " ${MSVC})
    message(STATUS "MSVC_IDE:                 " ${MSVC_IDE})
    message(STATUS "MSVC60:                   " ${MSVC60})
    message(STATUS "MSVC70:                   " ${MSVC70})
    message(STATUS "MSVC71:                   " ${MSVC71})
    message(STATUS "MSVC80:                   " ${MSVC80})
    message(STATUS "CMAKE_COMPILER_2005:      " ${CMAKE_COMPILER_2005})

    message(STATUS "CMAKE_BUILD_TYPE:         " ${CMAKE_BUILD_TYPE})
    message(STATUS "BUILD_SHARED_LIBS:        " ${BUILD_SHARED_LIBS})

    message(STATUS "CMAKE_C_FLAGS:            " ${CMAKE_C_FLAGS})
    message(STATUS "CMAKE_CXX_FLAGS:          " ${CMAKE_CXX_FLAGS})

    message(STATUS "CMAKE_C_COMPILER:         " ${CMAKE_C_COMPILER})
    message(STATUS "CMAKE_CXX_COMPILER:       " ${CMAKE_CXX_COMPILER})
    message(STATUS "CMAKE_COMPILER_IS_GNUCC:  " ${CMAKE_COMPILER_IS_GNUCC})
    message(STATUS "CMAKE_COMPILER_IS_GNUCXX: " ${CMAKE_COMPILER_IS_GNUCXX})

    message(STATUS "CMAKE_AR:                 " ${CMAKE_AR})
    message(STATUS "CMAKE_RANLIB:             " ${CMAKE_RANLIB})

    message(STATUS "CMAKE_SKIP_RULE_DEPENDENCY: " ${CMAKE_SKIP_RULE_DEPENDENCY})
    message(STATUS "CMAKE_SKIP_INSTALL_ALL_DEPENDENCY: " ${CMAKE_SKIP_INSTALL_ALL_DEPENDENCY})
    message(STATUS "CMAKE_SKIP_RPATH: " ${CMAKE_SKIP_RPATH})
    message(STATUS "CMAKE_VERBOSE_MAKEFILE: " ${CMAKE_VERBOSE_MAKEFILE})
    message(STATUS "CMAKE_SUPPRESS_REGENERATION: " ${CMAKE_SUPPRESS_REGENERATION})
endfunction()
