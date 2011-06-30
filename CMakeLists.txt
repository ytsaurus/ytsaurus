CMAKE_MINIMUM_REQUIRED(VERSION 2.6 FATAL_ERROR)
CMAKE_POLICY(VERSION 2.6.0)

# This variable defines a current root. PEERDIR, TOOLDIR, SUBDIR will use it
# as a root for their relative-path arguments.
SET(SOURCE_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/contrib/arcadia)

INCLUDE(${CMAKE_SOURCE_DIR}/contrib/arcadia/cmake/include/global.cmake NO_POLICY_SCOPE)

SET(ROOT_PROJECTS
    experiments
    unittests
    yt
    ytlib
)

ADD_DIRS(util)

IF (MAKE_ONLY)
    SET(__to_make_)
    FOREACH(__item_ ${MAKE_ONLY})
        IF ("${__item_}" STREQUAL "all")
            SET_APPEND(__to_make_ ${ROOT_PROJECTS})
        ELSE ("${__item_}" STREQUAL "all")
            IF (NOT EXISTS ${ARCADIA_ROOT}/${__item_})
                MESSAGE(SEND_ERROR "Directory \"${__item_}\" (listed in MAKE_ONLY) does not exist.")
            ELSE (NOT EXISTS ${ARCADIA_ROOT}/${__item_})
                SET_APPEND(__to_make_ ${__item_})
            ENDIF (NOT EXISTS ${ARCADIA_ROOT}/${__item_})
        ENDIF ("${__item_}" STREQUAL "all")
    ENDFOREACH(__item_ ${MAKE_ONLY})
    IF (__to_make_)
        SUBDIR(${__to_make_})
    ENDIF (__to_make_)
ELSE (MAKE_ONLY)
    SUBDIR(
        ${ROOT_PROJECTS}
    )
ENDIF (MAKE_ONLY)

PROJECT_EX(all_arcadia)

INCLUDE(${CMAKE_SOURCE_DIR}/cmake/include/dtmk.cmake)

#LEAVE_PROJECT()

IF (NOT WIN32)
    ADD_CUSTOM_TARGET(paths
        COMMAND echo "OS_NAME =" ${OS_NAME}
        COMMAND echo "OS_RELEASE =" ${OS_RELEASE}
        COMMAND echo "HARDWARE_TYPE =" ${HARDWARE_TYPE}
        COMMAND echo "ARCADIA_ROOT =" ${ARCADIA_ROOT}
        COMMAND echo "ARCADIA_BUILD_ROOT =" ${ARCADIA_BUILD_ROOT}
        COMMAND echo "CMAKE_BUILD_TYPE =" ${CMAKE_BUILD_TYPE}
        COMMAND echo "MAKE_ONLY =" ${MAKE_ONLY}

#       COMMAND echo "INSTALL =" ${INSTALL}
        COMMAND echo "INSTALLBASE =" ${INSTALLBASE}
        COMMAND echo "INSTALLFLAGS =" ${INSTALLFLAGS}
        COMMAND echo
        COMMAND echo "ALLOCATOR =" ${ALLOCATOR}
        COMMAND echo "USE_THREADS =" ${USE_THREADS}
        COMMAND echo "THREADLIB =" ${THREADLIB}
        COMMAND echo "PERLLIB =" ${PERLLIB}
        COMMAND echo "EXTUT =" ${EXTUT}
        COMMAND echo "PYTHON_VERSION =" ${PYTHON_VERSION}
        COMMAND echo "USE_ICONV =" ${USE_ICONV}
        COMMAND echo "CCVERS =" ${CCVERS}
        COMMAND echo "PKG_CREATE =" ${PKG_CREATE}
        COMMAND echo "ALLGCOV="${ALLGCOV}
    )

    ADD_CUSTOM_TARGET(versions
        COMMAND -echo "TOUCH: " ${TOUCH}
        COMMAND -echo "ECHO: " ${ECHO}
        COMMAND echo
        COMMAND -echo "AWK: " ${AWK}    && echo "" | ${AWK} --version | head -n 1
        COMMAND echo
        COMMAND -echo "BYACC: " ${BYACC} && ${BYACC} -V | head -n 1
        COMMAND echo
        COMMAND -echo "PERL: " ${PERL} && ${PERL} --version | sed -n '2,2p'
        COMMAND echo
        COMMAND -echo "PYTHON: " ${PYTHON} && ${PYTHON} --version
        COMMAND echo
        COMMAND -echo "CMAKE_C_COMPILER: " ${CMAKE_C_COMPILER} && ${CMAKE_C_COMPILER} -v
        COMMAND echo
        COMMAND -echo "CMAKE_CXX_COMPILER: " ${CMAKE_CXX_COMPILER}  && ${CMAKE_CXX_COMPILER} -v
        COMMAND echo
#       COMMAND -echo "LD_SO: " ${LD_SO} && ${LD_SO} -v
#       COMMAND echo
        COMMAND -echo "RAGEL: " ${RAGEL} && ${RAGEL}  -v
        COMMAND echo
        COMMAND -echo "RLGEN: " ${RLGEN} && ${RLGEN} -v
        COMMAND echo
        COMMAND -echo "RAGEL3: " ${RAGEL3} && ${RAGEL3} -v
        COMMAND echo
        COMMAND echo "OS: " ${OS_NAME}
    )

    ADD_CUSTOM_TARGET(gccversion
        COMMAND -echo "CMAKE_C_COMPILER: " ${CMAKE_C_COMPILER}  && ${CMAKE_C_COMPILER} -v
    )

    ADD_CUSTOM_TARGET(tags
        COMMAND find . -name "*.cpp" -o -name "*.h" -o -name "*.hh" -o -name "*.c" -o -name "*.cc" -o -name "*.l" -o -name "*.y" | ${CTAGS_PRG} --extra=+qf --fields=+aiS --c++-kinds=+p -L -
        WORKING_DIRECTORY ${ARCADIA_ROOT}
        )

ENDIF (NOT WIN32)

# we have the old CMake in extlib!
IF (NOT ROBOT_BUILD)

### This strings must locate only at the end of this file!!!
FILE(READ ${TEST_DART_TMP_FILENAME} test_dart_content)
FILE(WRITE ${TEST_DART_FILENAME} ${test_dart_content})
FILE(REMOVE ${TEST_DART_TMP_FILENAME})
###

ENDIF (NOT ROBOT_BUILD)

IF (COMMAND ON_CMAKE_FINISH_HOOK)
    ON_CMAKE_FINISH_HOOK()
ENDIF (COMMAND ON_CMAKE_FINISH_HOOK)
