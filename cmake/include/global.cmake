# First, remove per-session cached variables

GET_CMAKE_PROPERTY(vars CACHE_VARIABLES)
FOREACH(item ${vars})
    IF (item MATCHES "_(DEPEND|WEAK)NAME_(LIB|PROG)")
        SET(${item} "" CACHE INTERNAL "" FORCE)
    ENDIF (item MATCHES "_(DEPEND|WEAK)NAME_(LIB|PROG)")
ENDFOREACH(item ${vars})

# Some variables are set here (please find details at http://www.cmake.org/Wiki/CMake_Useful_Variables)
SET(CMAKE_SKIP_ASSEMBLY_SOURCE_RULES yes)
SET(CMAKE_SKIP_PREPROCESSED_SOURCE_RULES yes)
SET(CMAKE_SKIP_BUILD_RPATH yes)

IF (COMMAND cmake_policy)
    cmake_policy(SET CMP0002 OLD)
    cmake_policy(SET CMP0000 OLD)
    cmake_policy(SET CMP0005 OLD)
    #cmake_minimum_required(VERSION 2.6)
    cmake_policy(SET CMP0003 NEW)
ENDIF(COMMAND cmake_policy)

IF (NOT DEFINED DEBUG_MESSAGE_LEVEL AND DEFINED DML)
    SET(DEBUG_MESSAGE_LEVEL ${DML})
ENDIF (NOT DEFINED DEBUG_MESSAGE_LEVEL AND DEFINED DML)

# Saving ARCADIA_ROOT and ARCADIA_BUILD_ROOT into the cache

IF (LINUX)
    SET(REALPATH readlink)
    SET(REALPATH_FLAGS -f)
ELSEIF (FREEBSD)
    SET(REALPATH realpath)
    SET(REALPATH_FLAGS)
ENDIF (LINUX)

SET(SAVE_TEMPS $ENV{SAVE_TEMPS})

IF (NOT ARCADIA_ROOT)
    IF (0) #REALPATH)
        EXECUTE_PROCESS(COMMAND ${REALPATH} ${REALPATH_FLAGS} "${CMAKE_SOURCE_DIR}"
            OUTPUT_VARIABLE ARCADIA_ROOT
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        SET(CMAKE_CURRENT_SOURCE_DIR ${ARCADIA_ROOT})
        SET(CMAKE_SOURCE_DIR ${ARCADIA_ROOT})
    ELSE (0) #REALPATH)
        SET(ARCADIA_ROOT "${CMAKE_SOURCE_DIR}")
    ENDIF (0) #REALPATH)
    SET(ARCADIA_ROOT "${ARCADIA_ROOT}" CACHE PATH "Arcadia sources root dir" FORCE)
ENDIF (NOT ARCADIA_ROOT)

IF (NOT ARCADIA_BUILD_ROOT)
    IF (0) #REALPATH)
        EXECUTE_PROCESS(COMMAND ${REALPATH} ${REALPATH_FLAGS} "${CMAKE_BINARY_DIR}"
            OUTPUT_VARIABLE ARCADIA_BUILD_ROOT
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        SET(CMAKE_CURRENT_BINARY_DIR ${ARCADIA_BUILD_ROOT})
        SET(CMAKE_BINARY_DIR ${ARCADIA_BUILD_ROOT})
    ELSE (0) #REALPATH)
        SET(ARCADIA_BUILD_ROOT "${CMAKE_BINARY_DIR}")
    ENDIF (0) #REALPATH)
    SET(ARCADIA_BUILD_ROOT "${ARCADIA_BUILD_ROOT}" CACHE PATH "Arcadia binary root dir" FORCE)
ENDIF (NOT ARCADIA_BUILD_ROOT)

IF (WIN32)
    IF (NOT "X${YTEST_TARGET}X" STREQUAL "X${CMAKE_CURRENT_SOURCE_DIR}X")
        ADD_CUSTOM_TARGET(ytest python ${ARCADIA_ROOT}/check/run_test.py
            WORKING_DIRECTORY ${ARCADIA_BUILD_ROOT})
        SET_PROPERTY(TARGET "ytest" PROPERTY FOLDER "_CMakeTargets")
        SET (YTEST_TARGET ${CMAKE_CURRENT_SOURCE_DIR})
    ENDIF (NOT "X${YTEST_TARGET}X" STREQUAL "X${CMAKE_CURRENT_SOURCE_DIR}X")
ENDIF (WIN32)

# Here - only macros like SET_APPEND
INCLUDE(${ARCADIA_ROOT}/cmake/include/MakefileHelpers.cmake)
# Set OS_NAME and OS-specific variables
INCLUDE(${ARCADIA_ROOT}/cmake/include/config.cmake)

SET_PROPERTY(GLOBAL PROPERTY USE_FOLDERS ON)
SET_PROPERTY(GLOBAL PROPERTY PREDEFINED_TARGETS_FOLDER "_CMakeTargets")

SET(TEST_SCRIPTS_DIR ${ARCADIA_ROOT}/test)

IF (NOT WIN32)
    SET(PLATFORM_SUPPORTS_SYMLINKS yes)
    ENABLE(USE_WEAK_DEPENDS) # It should be enabled in build platforms with many-projects-at-once building capability
ELSE (NOT WIN32)
    # Force vcproj generator not to include CMakeLists.txt and its custom rules into the project
    SET_IF_NOTSET(CMAKE_SUPPRESS_REGENERATION yes)
    # To set IDE folder for an arbitrary project please define <project name>_IDE_FOLDER variable.
    # For example, SET(gperf_IDE_FOLDER "tools/build")
    DEFAULT(CMAKE_DEFAULT_IDE_FOLDER "<curdir>")
ENDIF (NOT WIN32)

INCLUDE(${ARCADIA_ROOT}/cmake/include/buildrules.cmake)
INCLUDE_FROM(local.cmake ${ARCADIA_ROOT}/.. ${ARCADIA_ROOT} ${ARCADIA_BUILD_ROOT}/.. ${ARCADIA_BUILD_ROOT})

IF (COMMAND ON_CMAKE_START_HOOK)
    ON_CMAKE_START_HOOK()
ENDIF (COMMAND ON_CMAKE_START_HOOK)

# Check that TMPDIR points to writable directory
SET(__tmpdir_ $ENV{TMPDIR})
IF (__tmpdir_)
    IF (NOT EXISTS ${__tmpdir_})
        MESSAGE(SEND_ERROR "TMPDIR env-variable is set (\"${__tmpdir_}\") but directory doesn't exist")
    ENDIF (NOT EXISTS ${__tmpdir_})
ENDIF (__tmpdir_)

# ============================================================================ #
# Here are global build variables (you can override them in your local.cmake)

IF (WIN32)
    SET_IF_NOTSET(INSTALLBASE       ./)
    SET_IF_NOTSET(INSTALL_ETC_DIR   ./)
    SET_IF_NOTSET(INSTALL_BIN_DIR   ./)
    SET_IF_NOTSET(INSTALL_LIB_DIR   ./)
    SET_IF_NOTSET(INSTALL_SBIN_DIR  ./)
    SET_IF_NOTSET(INSTALL_SHARE_DIR ./data)
    SET_IF_NOTSET(INSTALL_VAR_DIR   ./data)
ENDIF (WIN32)

IF (FREEBSD)
    SET_IF_NOTSET(INSTALL_LIB_DIR       ./usr/local/lib)      # http://www.freebsd.org/cgi/man.cgi?query=hier
ENDIF (FREEBSD)

IF (FREEBSD OR LINUX OR DARWIN)
    SET_IF_NOTSET(INSTALLBASE           ./usr/local)
    IF ( "${HARDWARE_TYPE}" MATCHES "x86_64")
        SET_IF_NOTSET(INSTALL_LIB_DIR ./usr/lib64)
    ENDIF ( "${HARDWARE_TYPE}" MATCHES "x86_64")
    SET_IF_NOTSET(INSTALL_LIB_DIR       ./usr/lib)                   # http://www.pathname.com/fhs/pub/fhs-2.3.html#USRLIBLIBRARIESFORPROGRAMMINGANDPA
    SET_IF_NOTSET(INSTALL_ETC_DIR       ${INSTALLBASE}/etc/yandex)   # http://www.debian.org/doc/debian-policy/ch-opersys.html#s9.1.2 && http://www.pathname.com/fhs/pub/fhs-2.3.html#USRLOCALLOCALHIERARCHY
    SET_IF_NOTSET(INSTALL_BIN_DIR       ${INSTALLBASE}/bin/)   # -||-
    SET_IF_NOTSET(INSTALL_SBIN_DIR      ${INSTALLBASE}/sbin/)  # -||-
    SET_IF_NOTSET(INSTALL_SHARE_DIR     ${INSTALLBASE}/share/yandex) # -||-
    SET_IF_NOTSET(INSTALL_VAR_DIR       ./var/local/yandex)          # http://www.pathname.com/fhs/pub/fhs-2.3.html#REQUIREMENTS12
ENDIF (FREEBSD OR LINUX OR DARWIN)

IF (DESTDIR)
    FILE(MAKE_DIRECTORY
        ${DESTDIR}${INSTALL_ETC_DIR}
        ${DESTDIR}${INSTALL_LIB_DIR}
        ${DESTDIR}${INSTALL_BIN_DIR}
        ${DESTDIR}${INSTALL_SBIN_DIR}
        ${DESTDIR}${INSTALL_SHARE_DIR}
        ${DESTDIR}${INSTALL_VAR_DIR}
    )
ENDIF (DESTDIR)

SET_IF_NOTSET(USE_J_ALLOCATOR yes)

IF ("${CMAKE_BUILD_TYPE}" STREQUAL "profile" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Profile" )
    DISABLE(USE_STATIC_CPP_RUNTIME)
ENDIF ("${CMAKE_BUILD_TYPE}" STREQUAL "profile" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Profile" )

SET_IF_NOTSET(USE_STATIC_CPP_RUNTIME yes)
SET_IF_NOTSET(LINK_STATIC_LIBS yes)
SET_IF_NOTSET(CHECK_TARGETPROPERTIES
#    USE_GOOGLE_ALLOCATOR
)
DEFAULT(UT_SUFFIX _ut)
IF (WIN32)
    DEFAULT(UT_PERDIR no)
ELSE (WIN32)
    DEFAULT(UT_PERDIR yes)
ENDIF (WIN32)
ENABLE(NO_LIBBIND)

SET(__USE_GENERATED_BYK_ $ENV{USE_GENERATED_BYK})
IF ("X${__USE_GENERATED_BYK_}X" STREQUAL "XX")
    DEFAULT(USE_GENERATED_BYK yes)
ELSE ("X${__USE_GENERATED_BYK_}X" STREQUAL "XX")
    DEFAULT(USE_GENERATED_BYK $ENV{USE_GENERATED_BYK})
ENDIF ("X${__USE_GENERATED_BYK_}X" STREQUAL "XX")

SET($ENV{LC_ALL} "C")

IF (WIN32)
    SET(EXECUTABLE_OUTPUT_PATH ${ARCADIA_BUILD_ROOT}/bin)
    SET(LIBRARY_OUTPUT_PATH    ${EXECUTABLE_OUTPUT_PATH})
ELSE (WIN32)
    SET(EXESYMLINK_DIR ${ARCADIA_BUILD_ROOT}/bin)
    SET(LIBSYMLINK_DIR ${ARCADIA_BUILD_ROOT}/lib)
ENDIF (WIN32)

IF (NO_UT_EXCLUDE_FROM_ALL)
    SET(UT_EXCLUDE_FROM_ALL "")
ELSE (NO_UT_EXCLUDE_FROM_ALL)
    SET(UT_EXCLUDE_FROM_ALL "EXCLUDE_FROM_ALL")
ENDIF (NO_UT_EXCLUDE_FROM_ALL)

# End of global build variables list

INCLUDE(${ARCADIA_ROOT}/cmake/include/tools.cmake)
INCLUDE(${ARCADIA_ROOT}/cmake/include/suffixes.cmake)

# Macro - get varname from ENV
MACRO(GETVAR_FROM_ENV)
    FOREACH (varname ${ARGN})
        IF (NOT DEFINED ${varname})
            SET(${varname} $ENV{${varname}})
        ENDIF (NOT DEFINED ${varname})
        DEBUGMESSAGE(1, "${varname}=${${varname}}")
    ENDFOREACH (varname)
ENDMACRO(GETVAR_FROM_ENV)

# Get some vars from ENV
GETVAR_FROM_ENV(MAKE_CHECK MAKE_ONLY USE_DISTCC USE_TIME COMPILER_PREFIX NOSTRIP)

IF (NOT MAKE_CHECK)
    MESSAGE(STATUS "MAKE_CHECK is negative")
ENDIF (NOT MAKE_CHECK)

IF (MAKE_RELEASE OR DEFINED release)
    SET(CMAKE_BUILD_TYPE Release)
ELSEIF (MAKE_COVERAGE OR DEFINED coverage)
    SET(CMAKE_BUILD_TYPE Coverage)
ELSEIF (MAKE_COVERAGE OR DEFINED profile)
    SET(CMAKE_BUILD_TYPE Profile)
ELSEIF (MAKE_VALGRIND OR DEFINED valgrind)
    SET(CMAKE_BUILD_TYPE Valgrind)
ELSEIF (DEFINED debug)
    SET(CMAKE_BUILD_TYPE Debug)
ELSE (MAKE_RELEASE OR DEFINED release)
    # Leaving CMAKE_BUILD_TYPE intact
ENDIF (MAKE_RELEASE OR DEFINED release)

# Default build type - DEBUG
IF (NOT WIN32 AND NOT CMAKE_BUILD_TYPE)
    SET(CMAKE_BUILD_TYPE Debug)
ENDIF (NOT WIN32 AND NOT CMAKE_BUILD_TYPE)

IF (NOT "X${CMAKE_BUILD_TYPE}X" STREQUAL "XX")
    # Enable MAKE_<config> to simplify checks
    STRING(TOUPPER "${CMAKE_BUILD_TYPE}" __upcase_)
    ENABLE(MAKE_${__upcase_})
ENDIF (NOT "X${CMAKE_BUILD_TYPE}X" STREQUAL "XX")

MACRO (CACHE_VAR __cached_varname_ __argn_varname_ varname type descr)
    IF (DEFINED ${varname})
        SET(${__cached_varname_} "${${__cached_varname_}} ${varname}[${${varname}}]")
        SET(${varname} "${${varname}}" CACHE ${type} "${descr}" FORCE)
        DEBUGMESSAGE(1, "Caching ${varname}[${${varname}}]")
    ENDIF (DEFINED ${varname})
    SET(${__argn_varname_} ${ARGN})
ENDMACRO (CACHE_VAR varname type string)

MACRO (CACHE_VARS)
    SET(__cached_)
    SET(__vars_ ${ARGN})
    WHILE(NOT "X${__vars_}X" STREQUAL "XX")
        CACHE_VAR(__cached_ __vars_ ${__vars_})
    ENDWHILE(NOT "X${__vars_}X" STREQUAL "XX")
    IF (__cached_)
        MESSAGE(STATUS "Cached:${__cached_}")
    ENDIF (__cached_)
ENDMACRO (CACHE_VARS)

SET(METALIBRARY_NAME CMakeLists.lib)
SET(METALIB_LIST "")

# Save CMAKE_BUILD_TYPE, MAKE_CHECK etc.
CACHE_VARS(
    MAKE_ONLY STRING "Used to strip buildtree. May contain zero or more paths relative to ARCADIA_ROOT"
    MAKE_CHECK BOOL "Excludes projects with positive NOCHECK"
    CMAKE_BUILD_TYPE STRING "Build type (Release, Debug, Valgrind, Profile, Coverage)"
    USE_CCACHE BOOL "Adds 'ccache' prefix to c/c++ compiler line"
    USE_DISTCC BOOL "Adds 'distcc' prefix to c/c++ compiler line"
    USE_TIME BOOL "Adds 'time' prefix to c/c++ compiler line"
    COMPILER_PREFIX STRING "Adds arbitrary prefix to c/c++ compiler line"
)

# processed_dirs.txt holds all processed directories
SET(PROCESSED_DIRS_FILE ${ARCADIA_BUILD_ROOT}/processed_dirs.txt)
FILE(WRITE "${PROCESSED_DIRS_FILE}" "Empty\n")
SET(PROCESSED_TARGETS_FILE ${ARCADIA_BUILD_ROOT}/processed_targets.txt)
FILE(WRITE "${PROCESSED_TARGETS_FILE}" "Empty\n")

# This file holds <target name> <source path> <target file name> for all targets
SET_IF_NOTSET(TARGET_LIST_FILENAME "${ARCADIA_BUILD_ROOT}/target.list")
FILE(WRITE "${TARGET_LIST_FILENAME}" "")
SET_IF_NOTSET(EXCLTARGET_LIST_FILENAME "${ARCADIA_BUILD_ROOT}/excl_target.list")
FILE(WRITE "${EXCLTARGET_LIST_FILENAME}" "")
SET_IF_NOTSET(TEST_LIST_FILENAME "${ARCADIA_BUILD_ROOT}/test.list")
FILE(WRITE "${TEST_LIST_FILENAME}" "")
IF (NOT DEFINED TEST_DART_TMP_FILENAME)
        SET(TEST_DART_TMP_FILENAME "${ARCADIA_BUILD_ROOT}/__test.dart.tmp")
        FILE(APPEND "${TEST_DART_TMP_FILENAME}" "=============================================================\n")
ENDIF (NOT DEFINED TEST_DART_TMP_FILENAME)
SET_IF_NOTSET(TEST_DART_FILENAME "${ARCADIA_BUILD_ROOT}/test.dart")
DEFAULT(UNITTEST_LIST_FILENAME "${ARCADIA_BUILD_ROOT}/unittest.list")
FILE(WRITE "${UNITTEST_LIST_FILENAME}" "")

SET_IF_NOTSET(USE_OWNERS yes)
IF (USE_OWNERS)
    SET_IF_NOTSET(PROCESSED_OWNERS_FILE ${ARCADIA_BUILD_ROOT}/owners.list)
ENDIF (USE_OWNERS)


# c/c++ flags, compilers, etc.

IF (COMPILER_PREFIX)
    SET(CMAKE_CXX_COMPILE_OBJECT "${COMPILER_PREFIX} ${CMAKE_CXX_COMPILE_OBJECT}")
    SET(CMAKE_C_COMPILE_OBJECT "${COMPILER_PREFIX} ${CMAKE_C_COMPILE_OBJECT}")
    DEBUGMESSAGE(1, "COMPILER_PREFIX=${COMPILER_PREFIX}")
ENDIF (COMPILER_PREFIX)

IF (USE_DISTCC)
    IF (USE_CCACHE)
        SET(CMAKE_CXX_COMPILE_OBJECT "CCACHE_PREFIX=distcc ${CMAKE_CXX_COMPILE_OBJECT}")
        SET(CMAKE_C_COMPILE_OBJECT   "CCACHE_PREFIX=distcc ${CMAKE_C_COMPILE_OBJECT}")
        DEBUGMESSAGE(1, "USE_DISTCC: set distcc as ccache prefix")
    ELSE (USE_CCACHE)
        SET(CMAKE_CXX_COMPILE_OBJECT "distcc ${CMAKE_CXX_COMPILE_OBJECT}")
        SET(CMAKE_C_COMPILE_OBJECT   "distcc ${CMAKE_C_COMPILE_OBJECT}")
        DEBUGMESSAGE(1, "USE_DISTCC: set distcc as compiler prefix")
    ENDIF (USE_CCACHE)

    DEBUGMESSAGE(1, "USE_DISTCC=${USE_DISTCC}")
ENDIF (USE_DISTCC)

IF (USE_TIME)
    SET(__time_compile_logfile_ ${ARCADIA_BUILD_ROOT}/time.compile.log)
    SET(__time_link_logfile_    ${ARCADIA_BUILD_ROOT}/time.link.log)
    DEFAULT(TIME "/usr/bin/time")
    FOREACH(__i_ CMAKE_C_COMPILE_OBJECT CMAKE_CXX_COMPILE_OBJECT)
        SET(${__i_} "${TIME} -ap -o ${__time_compile_logfile_} ${${__i_}}")
#       SET(${__i_} "echo 'file <SOURCE>' >> ${__time_compile_logfile_}; ${TIME} -ap -o ${__time_compile_logfile_} ${${__i_}}")
    ENDFOREACH(__i_)
    FOREACH(__i_ CMAKE_C_LINK_EXECUTABLE CMAKE_CXX_LINK_EXECUTABLE CMAKE_C_CREATE_SHARED_LIBRARY CMAKE_CXX_CREATE_SHARED_LIBRARY)
        SET(${__i_} "${TIME} -ap -o ${__time_link_logfile_} ${${__i_}}")
#       SET(${__i_} "echo 'target <TARGET>' >> ${__time_link_logfile_}; ${TIME} -ap -o ${__time_link_logfile_} ${${__i_}}")
    ENDFOREACH(__i_)
    DEBUGMESSAGE(1, "USE_TIME=${USE_TIME}")
ENDIF (USE_TIME)

INCLUDE(${ARCADIA_ROOT}/cmake/include/codgen.cmake)
INCLUDE(${ARCADIA_ROOT}/cmake/include/FindRagel.cmake)
SET(__use_perl_ ${USE_PERL})
ENABLE(USE_PERL)
INCLUDE(${ARCADIA_ROOT}/cmake/include/FindPerl2.cmake)
SET(USE_PERL ${__use_perl_})
INCLUDE(${ARCADIA_ROOT}/cmake/include/UseSwig.cmake)

IF (NOT DEFINED PRINTCURPROJSTACK)
    IF ("${DEBUG_MESSAGE_LEVEL}" GREATER "0")
        SET(PRINTCURPROJSTACK yes CACHE INTERNAL "")
    ELSE ("${DEBUG_MESSAGE_LEVEL}" GREATER "0")
        SET(PRINTCURPROJSTACK no)
    ENDIF ("${DEBUG_MESSAGE_LEVEL}" GREATER "0")
ENDIF (NOT DEFINED PRINTCURPROJSTACK)

DEFAULT(DICTIONARIES_ROOT ${ARCADIA_ROOT}/../dictionaries)

ADD_CUSTOM_TARGET(all_ut)
SET_PROPERTY(TARGET "all_ut" PROPERTY FOLDER "_CMakeTargets")

# Use cmake exclusion list, optional
DEFAULT(EXCLUDE_PROJECTS "")
IF (EXISTS ${ARCADIA_BUILD_ROOT}/exclude.cmake)
    FILE(STRINGS ${ARCADIA_BUILD_ROOT}/exclude.cmake EXCLUDE_PROJECTS)
ENDIF (EXISTS ${ARCADIA_BUILD_ROOT}/exclude.cmake)
DEBUGMESSAGE(1 "exclude list: ${EXCLUDE_PROJECTS}")


