#This file contains macros which prepare variables and define macros used later in cmake-file

IF (NOT ARCADIA_ROOT)
    # At this stage it is absolutely mandatory to have this variable defined
    MESSAGE(FATAL_ERROR "ARCADIA_ROOT is not defined")
ENDIF (NOT ARCADIA_ROOT)

INCLUDE(${ARCADIA_ROOT}/cmake/include/dtmk.cmake)

# =============================================================================
# PEERDIR(dirs)
#
MACRO (PEERDIR)
    SET(__peerdirs_)
    SET(__next_addincl_)
    FOREACH(__item_ ${ARGN})
        IF ("${__item_}" STREQUAL "ADDINCL")
            SET(__next_addincl_ yes)
        ELSE ("${__item_}" STREQUAL "ADDINCL")
            SET_APPEND(__peerdirs_ ${__item_})
            # FILE( GLOB __realdir_ RELATIVE ${ARCADIA_ROOT} ${__item_} )
            FILE( GLOB __realdir_ ${ARCADIA_ROOT}/${__item_} )
            IF(__realdir_)
                DEBUGMESSAGE(3 "PEERDIR( ${__item_} ) is found (${__realdir_})")
            ELSE(__realdir_)
                MESSAGE(SEND_ERROR "PEERDIR( ${__item_} ) does not exist (in ${CURDIR})")
            ENDIF(__realdir_)

            IF (__next_addincl_)
                SET_APPEND(DTMK_I ${ARCADIA_ROOT}/${__item_} ${ARCADIA_BUILD_ROOT}/${__item_})
                SET(__next_addincl_)
            ENDIF (__next_addincl_)
        ENDIF ("${__item_}" STREQUAL "ADDINCL")
    ENDFOREACH(__item_)
    ADD_DIRS(PEERDIR ${__peerdirs_})
ENDMACRO (PEERDIR)

# =============================================================================
# WEAK(libpath)
#
MACRO (WEAK libpath)
    SET(LOCAL_WEAKNAME_LIB ${libpath})
ENDMACRO (WEAK)

# =============================================================================
# TOOLDIR_EX(pairs of <dir varname> )
#
MACRO (TOOLDIR_EX)
    SET(__isdir_ yes)
    SET(__dirs_)
    SET(__vars_)
    FOREACH(__item_ ${ARGN})
        IF (__isdir_)
            SET(__lastdir_ "${__item_}")
            SET(__isdir_ no)
        ELSE (__isdir_)
            SET_APPEND(__dirs_ "${__lastdir_}")
            SET(__lastdir_)
            SET_APPEND(__vars_ "${__item_}")
            SET(__isdir_ yes)
        ENDIF (__isdir_)
    ENDFOREACH(__item_)
    IF (__lastdir_)
        MESSAGE(SEND_ERROR "TOOLDIR_EX @ ${CMAKE_CURRENT_SOURCE_DIR} has odd number of parameters (last is [${__lastdir_}])")
    ENDIF (__lastdir_)

    ADD_DIRS(TOOLDIR ${__dirs_})

    FOREACH(__item_ ${__dirs_})
        LIST(GET __vars_ 0 __var_)
        LIST(REMOVE_AT __vars_ 0)
        GET_GLOBAL_DIRNAME(__dir_ ${SOURCE_ROOT}/${__item_})
        IF (NOT "X${${__dir_}_DEPENDNAME_PROG}X" STREQUAL "XX")
            GETTARGETNAME(${__var_} ${${__dir_}_DEPENDNAME_PROG})
            DEBUGMESSAGE(2 "TOOLDIR in ${__item_} is [${${__dir_}_DEPENDNAME_PROG}]")
        ELSE (NOT "X${${__dir_}_DEPENDNAME_PROG}X" STREQUAL "XX")
            SET(${__var_} NOTFOUND)
            MESSAGE(SEND_ERROR "TOOLDIR in ${__item_} is not defined")
        ENDIF (NOT "X${${__dir_}_DEPENDNAME_PROG}X" STREQUAL "XX")
    ENDFOREACH(__item_)
ENDMACRO (TOOLDIR_EX)

# =============================================================================
# TOOLDIR(dirs)
#
MACRO (TOOLDIR)
    ADD_DIRS(TOOLDIR ${ARGN})
ENDMACRO (TOOLDIR)

# =============================================================================
# SUBDIR(dirs)
#
MACRO (SUBDIR)
    ADD_DIRS(#SUBDIR
        ${ARGN}
    )
ENDMACRO (SUBDIR)

# =============================================================================
# SRCDIR(dirs)
#
MACRO (SRCDIR)
    SET_APPEND(SRCDIR ${ARGN})
ENDMACRO (SRCDIR)

# =============================================================================
# ADDINCL(dirs) - adds directories to -I switches
#
MACRO (ADDINCL)
    FOREACH(__item_ in ${ARGN})
        SET_APPEND(DTMK_I ${ARCADIA_ROOT}/${__item_} ${ARCADIA_BUILD_ROOT}/${__item_})
    ENDFOREACH(__item_)
ENDMACRO (ADDINCL)

# =============================================================================
# CFLAGS(flags), CONLYFLAGS(flags) and CXXFLAGS(flags)
#
MACRO (CFLAGS)
    SET_APPEND(CFLAGS ${ARGN})
ENDMACRO (CFLAGS)

MACRO (CONLYFLAGS)
    SET_APPEND(CONLYFLAGS ${ARGN})
ENDMACRO (CONLYFLAGS)

MACRO (CXXFLAGS)
    SET_APPEND(CXXFLAGS ${ARGN})
ENDMACRO (CXXFLAGS)

# =============================================================================
# SRCS(prjname)
#
MACRO (SRCS)
    # parse ARGN, get

    SET(__next_prop_)
    SET(__cursrc_)
    SET(__flagnames_ COMPILE_FLAGS OBJECT_DEPENDS CODGENFLAGS DEPENDS PROTO_RELATIVE)
    FOREACH(__item_ ${ARGN})
        IF (__next_prop_)
            IF (NOT __cursrc_)
                MESSAGE(FATAL_ERROR "No source file name found to set ${__next_prop_} property")
            ENDIF (NOT __cursrc_)
            # Set property for a given file
            GET_GLOBAL_DIRNAME(__item_global_ "${__cursrc_}")
            SET_APPEND(${__item_global_}_PROPS "${__next_prop_};${__item_}")
            DEBUGMESSAGE(1 "${__item_global_}_PROPS[${${__item_global_}_PROPS}]")
            SET(__next_prop_)
        ELSE (__next_prop_)
            SET(__is_flag_)
            FOREACH(__flagname_ ${__flagnames_})
                IF (__item_ STREQUAL "${__flagname_}")
                    SET(__is_flag_ yes)
                ENDIF (__item_ STREQUAL "${__flagname_}")
            ENDFOREACH(__flagname_)

            IF (__is_flag_)
                SET(__next_prop_ "${__item_}")
            ELSE (__is_flag_)
                SET_APPEND(SRCS "${__item_}")
                SET(__cursrc_ "${__item_}")
            ENDIF (__is_flag_)
        ENDIF (__next_prop_)
    ENDFOREACH(__item_)
ENDMACRO (SRCS)

# =============================================================================
# SRCS_NOCOMPILE(foldername)
# Macro will add listed any source files into <foldername> in MSVC
#   while not compiling them even if they are c/cpp.
#
MACRO (SRCS_NOCOMPILE foldername)
    IF (WIN32)
        FOREACH(__srcfileName_ ${ARGN})
            GET_FILENAME_COMPONENT(__name_ ${__srcfileName_} NAME)
            ADD_CUSTOM_COMMAND(
                  OUTPUT ${BINDIR}/${__name_}.fake.h
                  COMMAND echo 1 > ${BINDIR}/${__name_}.fake.h
                  MAIN_DEPENDENCY ${__srcfileName_}
                  COMMENT ""
                  WORKING_DIRECTORY ${BINDIR}
            )
            SOURCE_GROUP("${foldername}" FILES ${__srcfileName_})
            SOURCE_GROUP("${foldername}/Fake" FILES ${BINDIR}/${__name_}.fake.h)
            SRCS(${BINDIR}/${__name_}.fake.h)
        ENDFOREACH(__srcfileName_)
    ENDIF (WIN32)
ENDMACRO (SRCS_NOCOMPILE)

MACRO (RECURSE)
    ENTER_PROJECT()
    SUBDIR(${ARGN})
    LEAVE_PROJECT()
ENDMACRO (RECURSE)

# =============================================================================
# BUILD(variables list...) and NOBUILD(variables list...)
#   Use BUILD macro when you need to create a project only when 1+ of listed
# variables is positive.
#   Use NOBUILD macro when you don't want a project to be created if any of
# listed variables is positive. NOBUILD has more priority.
#
#   See ${EXCLTARGET_LIST_FILENAME} for a list of excluded targets after
# cmake configure stage (defaulted to ${ARCADIA_BUILD_ROOT}/excl_target.list).

MACRO(BUILD)
    SET_APPEND(BUILD_WHEN_VARS ${ARGN})
ENDMACRO(BUILD)

MACRO(NOBUILD)
    SET_APPEND(NOBUILD_WHEN_VARS ${ARGN})
ENDMACRO(NOBUILD)

MACRO(EXCLUDE_FROM_ALL)
    SET(LOCAL_EXCLUDE_FROM_ALL yes)
ENDMACRO(EXCLUDE_FROM_ALL)

# =============================================================================
# BUILDAFTER(dirs ...)
#
MACRO (BUILDAFTER)
    ADD_DIRS(${ARGN})
    FOREACH(__item_ ${ARGN})
        GET_GLOBAL_DIRNAME(__dir_ ${SOURCE_ROOT}/${__item_})
        ENABLE(__invalid_)
        FOREACH(__depend_ ${__dir_}_DEPENDNAME_PROG ${__dir_}_DEPENDNAME_LIB)
            IF (NOT "X${${__depend_}}X" STREQUAL "XX")
                LIST(APPEND THISPROJECTDEPENDS ${${__depend_}})
                DEBUGMESSAGE(2 "BUILDAFTER @ ${CURDIR}: ${__item_} is [${${__depend_}}]")
                DISABLE(__invalid_)
            ENDIF (NOT "X${${__depend_}}X" STREQUAL "XX")
        ENDFOREACH(__depend_)
        IF (__invalid_)
            MESSAGE(SEND_ERROR "BUILDAFTER: not found a target in [${__item_}] for [${CURDIR}]")
        ENDIF (__invalid_)
    ENDFOREACH(__item_)
ENDMACRO (BUILDAFTER)

# =============================================================================
# ENTER_PROJECT(prjname)
#
MACRO (ENTER_PROJECT)
    IF (PRINTCURPROJSTACK)
        IF (ARGN)
            SET(__prjname_ ${ARGN})
        ELSE (ARGN)
            FILE(RELATIVE_PATH __var_ "${ARCADIA_ROOT}" "${CMAKE_CURRENT_SOURCE_DIR}/CMakeLists.txt")
            GET_FILENAME_COMPONENT(__prjname_ ${__var_} PATH)
        ENDIF (ARGN)
        SET(CURPROJSTACK ${__prjname_} ${CURPROJSTACK})
        DEBUGMESSAGE(2 "Enter. Current project stack: [${CURPROJSTACK}]")
    ELSE (PRINTCURPROJSTACK)
        DEBUGMESSAGE(2 "PRINTCURPROJSTACK is \"${PRINTCURPROJSTACK}\"")
    ENDIF (PRINTCURPROJSTACK)
ENDMACRO (ENTER_PROJECT)

# =============================================================================
# LEAVE_PROJECT()
#
MACRO (LEAVE_PROJECT)
    IF (PRINTCURPROJSTACK)
        DEBUGMESSAGE(2 "Exit.  Current project stack: [${CURPROJSTACK}]")
        LIST(REMOVE_AT CURPROJSTACK 0)
    ELSE (PRINTCURPROJSTACK)
    ENDIF (PRINTCURPROJSTACK)
ENDMACRO (LEAVE_PROJECT)


#IF (NOT DEFINED ARCADIA_BUILD_ROOT OR NOT EXISTS ${ARCADIA_BUILD_ROOT})
#   IF (PATH_TO_ROOT)
#       SET(ARCADIA_BUILD_ROOT ${CMAKE_CURRENT_BINARY_DIR}/${PATH_TO_ROOT}) # Assume that first cmake ran in
#       GET_FILENAME_COMPONENT(ARCADIA_BUILD_ROOT ${ARCADIA_BUILD_ROOT} ABSOLUTE)
#   ELSE (PATH_TO_ROOT)
#       SET(ARCADIA_BUILD_ROOT ${CMAKE_CURRENT_BINARY_DIR}) # Assume that first cmake ran in
#       GET_FILENAME_COMPONENT(ARCADIA_BUILD_ROOT ${ARCADIA_BUILD_ROOT} ABSOLUTE)
#       SET(ARCADIA_BUILD_ROOT ${CMAKE_CURRENT_BINARY_DIR} CACHE INTERNAL "")
#   ENDIF (PATH_TO_ROOT)
#ENDIF (NOT DEFINED ARCADIA_BUILD_ROOT OR NOT EXISTS ${ARCADIA_BUILD_ROOT})

#MESSAGE("ARCADIA_ROOT is [" ${ARCADIA_ROOT} "]")
#MESSAGE("ARCADIA_BUILD_ROOT is [" ${ARCADIA_BUILD_ROOT} "]")

# This macro checks whether a directory contains metalibrary structure
MACRO (IS_METALIBRARY result_varname path)
    SET(${result_varname} no)
    IF (1 AND EXISTS ${ARCADIA_ROOT}/${path}/${METALIBRARY_NAME} AND NOT EXISTS ${ARCADIA_ROOT}/${path}/CMakeLists.txt)
        SET(${result_varname} yes)
    ENDIF (1 AND EXISTS ${ARCADIA_ROOT}/${path}/${METALIBRARY_NAME} AND NOT EXISTS ${ARCADIA_ROOT}/${path}/CMakeLists.txt)
ENDMACRO (IS_METALIBRARY)

# =============================================================================
# ADD_ARC_DIRS(dirname)
#
#
MACRO (ADD_ARC_DIRS var_to_remove)
    DEBUGMESSAGE(2 "${CURDIR}: PEERDIR[${PEERDIR}], TOOLDIR[${TOOLDIR}], SUBDIR[${SUBDIR}], ARGN[${ARGN}]")

    SET(__peerdir_ ${PEERDIR})
    SET(__subdir_ ${SUBDIR})
    SET(__tooldir_ ${TOOLDIR})
    SET(__prj_def_ ${PROJECT_DEFINED})
    SET(PEERDIR)
    SET(SUBDIR)
    SET(TOOLDIR)
    SET(PROJECT_DEFINED)
    SET(ADD_PEERDIR)
    FOREACH(__item_ ${ARGN})
        IS_METALIBRARY(__is_meta_ ${__item_})
        IF (__is_meta_ AND "${__fullvarname_}" MATCHES "PEERDIR")
            SET_APPEND(ADD_PEERDIR ${__item_})
            DEBUGMESSAGE(1 "Metalib in ${__item_}")
        ELSE (__is_meta_ AND "${__fullvarname_}" MATCHES "PEERDIR")
            ADD_SUBDIRECTORY_EX(${ARCADIA_ROOT}/${__item_} ${ARCADIA_BUILD_ROOT}/${__item_})
        ENDIF (__is_meta_ AND "${__fullvarname_}" MATCHES "PEERDIR")
    ENDFOREACH(__item_)
    SET(PEERDIR ${__peerdir_})
    SET(SUBDIR ${__subdir_})
    SET(TOOLDIR ${__tooldir_})
    SET(PROJECT_DEFINED ${__prj_def_})

    IF (ADD_PEERDIR)
        DEBUGMESSAGE(1 "ADD_PEERDIR[${ADD_PEERDIR}]")
        SET(${SAVEDNAME}_ADD_PEERDIR ${ADD_PEERDIR})
    ENDIF (ADD_PEERDIR)

    # Now include all collected metalib files
    FOREACH(__item_ ${ADD_PEERDIR})
        SET(__metaname_ ${ARCADIA_ROOT}/${__item_}/${METALIBRARY_NAME})
        SAVE_VARIABLES(SAVEDNAME PROJECT_DEFINED)
        DEBUGMESSAGE(2 "METALIBRARY: INCLUDE(${__metaname_} @ ${CURDIR}")
        INCLUDE(${__metaname_})
        CHECK_SAVED_VARIABLES(__changed_ SAVEDNAME PROJECT_DEFINED)
        IF (__changed_)
            MESSAGE(SEND_ERROR "buildrules.cmake: ${__metaname_} modified following variables: [${__changed_}].")
            MESSAGE(SEND_ERROR "buildrules.cmake: Possibly it defines a target and it is prohibited for .lib-files.")
        ENDIF (__changed_)
        SRCDIR(${__item_})
        SET_APPEND(DTMK_I ${__item_})
    ENDFOREACH(__item_)

    IF (${SAVEDNAME}_ADD_PEERDIR)
        SET(${var_to_remove} ${${SAVEDNAME}_ADD_PEERDIR})
        DEBUGMESSAGE(1 "ADD_PEERDIR to remove: ${${var_to_remove}}")
    ENDIF (${SAVEDNAME}_ADD_PEERDIR)
ENDMACRO (ADD_ARC_DIRS)

# =============================================================================
# ADD_DIRS( (PEERDIR | SUBDIR | TOOLDIR) ...)
#
#
MACRO (ADD_DIRS varname)
    SET(__varname_)
    STRING(REGEX MATCH "DIR" __varname_ ${varname})
    SET(__fullvarname_ ${varname})

    DEBUGMESSAGE(2 "__varname_[${__varname_}]")
    SET(__to_remove_)
    IF ("${__varname_}" STREQUAL "DIR")
#       MESSAGE("Matches (@ ${CMAKE_CURRENT_SOURCE_DIR})")
        ADD_ARC_DIRS(__to_remove_ ${ARGN})
        SET_APPEND(${varname} ${ARGN})
#        DEBUGMESSAGE(1 "__to_remove_[${__to_remove_}]")
        IF (__to_remove_)
            LIST(REMOVE_ITEM ${varname} ${__to_remove_})
            DEBUGMESSAGE(1 "Removing dirs from PEERDIR [${__to_remove_}]")
            DEBUGMESSAGE(1 "The rest is [${varname}]")
        ENDIF (__to_remove_)
    ELSE ("${__varname_}" STREQUAL "DIR")
        ADD_ARC_DIRS(__to_remove_ ${varname} ${ARGN})
    ENDIF ("${__varname_}" STREQUAL "DIR")
ENDMACRO (ADD_DIRS)

# =============================================================================
# ADD_SUBDIRECTORY_EX(srcdir bindir)
#
#
MACRO (ADD_SUBDIRECTORY_EX srcdir bindir)
    SET(__dir_found_ no)

    FILE(READ ${PROCESSED_DIRS_FILE} PROCESSED_DIRS)
    IF ("${PROCESSED_DIRS}" MATCHES "X${srcdir}X")
        SET(__dir_found_ yes)
    ENDIF ("${PROCESSED_DIRS}" MATCHES "X${srcdir}X")

    IF (__dir_found_)
        DEBUGMESSAGE(2 "======= Directory [${srcdir}] is already processed. Skipping")
    ELSE (__dir_found_)
        IF (NOT EXISTS ${srcdir}/CMakeLists.txt)
            DEBUGMESSAGE(2 "=====!! File [${srcdir}/CMakeLists.txt] does not exist. Skipping")
        ELSE (NOT EXISTS ${srcdir}/CMakeLists.txt)
            # check if subdir is excluded due to cmake errors
            STRING(REPLACE ${ARCADIA_ROOT}/ "" canonpath ${srcdir})
            LIST(FIND EXCLUDE_PROJECTS ${canonpath} exclude_idx)
            IF (exclude_idx LESS 0) # i.e. canonpath is not in exclude list
                FILE(APPEND ${PROCESSED_DIRS_FILE} "X${srcdir}X;")
                ADD_SUBDIRECTORY(${srcdir} ${bindir})
            ELSE (exclude_idx LESS 0) 
                DEBUGMESSAGE(0 "project ${canonpath} is manualy excluded from build")
            ENDIF (exclude_idx LESS 0)
        ENDIF (NOT EXISTS ${srcdir}/CMakeLists.txt)
    ENDIF (__dir_found_)
ENDMACRO (ADD_SUBDIRECTORY_EX)

# =============================================================================
# ADD_CUSTOM_TARGET_EX(target)
#  This macro tracks target names to avoid duplicates (duplicates lead to
#   problems that are not solved easily)
#
MACRO (ADD_CUSTOM_TARGET_EX target)
    IF (PROCESSED_TARGETS_FILE)
        SET(__dir_found_ no)
        EXECUTE_PROCESS(COMMAND cat ${PROCESSED_TARGETS_FILE}
            COMMAND grep "X${target}X"
            RESULT_VARIABLE __dir_found_
            OUTPUT_VARIABLE __dev_null_)
        IF (NOT __dir_found_)
            MESSAGE(SEND_ERROR "buildrules.cmake: TARGET[${target}] is already defined in this build tree. Please see messages in debugmessagelevel 2 for details")
        ELSE (NOT __dir_found_)
            FILE(APPEND ${PROCESSED_TARGETS_FILE} "X${target}X\n")
            DEBUGMESSAGE(2 "buildrules.cmake: Adding TARGET[${target}] in ${CMAKE_CURRENT_SOURCE_DIR}")
            ADD_CUSTOM_TARGET(${target} ${ARGN})
        ENDIF (NOT __dir_found_)
    ELSE (PROCESSED_TARGETS_FILE)
        DEBUGMESSAGE(1 "buildrules.cmake: Warning: PROCESSED_TARGETS_FILE variable is empty. Please set it to be globally visible")
        ADD_CUSTOM_TARGET(${target} ${ARGN})
    ENDIF (PROCESSED_TARGETS_FILE)
ENDMACRO (ADD_CUSTOM_TARGET_EX)

# =============================================================================
# ADD_CUSTOM_COMMAND_EX()
#  This macro adds dependency to the current custom rule definition.
#
MACRO (ADD_CUSTOM_COMMAND_EX)
    SET(__rule_filename_)
    SET(__outfile_found_ no)
    FOREACH(__item_ ${ARGN})
        IF (NOT __outfile_found_ AND NOT "${__rule_filename_}" STREQUAL "OUTPUT")
            SET(__rule_filename_ ${__item_})
        ELSEIF (NOT __outfile_found_ )
            SET(__rule_filename_ ${__item_})
            SET(__outfile_found_ yes)
        ENDIF (NOT __outfile_found_ AND NOT "${__rule_filename_}" STREQUAL "OUTPUT")
    ENDFOREACH(__item_)
    SET(__new_cmd_ "${ARGN}")
    IF (__outfile_found_)
        IS_FULLNAME("${__rule_filename_}" __is_full_)
        SET(__rule_suffix_ ".custom_build_rule")

        IF (__is_full_)
            SET(__rule_filename_ "${__rule_filename_}${__rule_suffix_}")
        ELSE (__is_full_)
            SET(__rule_filename_ "${BINDIR}/${__rule_filename_}${__rule_suffix_}")
        ENDIF (__is_full_)

        SET(__old_rule_)
        IF (EXISTS "${__rule_filename_}")
            FILE(READ "${__rule_filename_}" __old_rule_)
        ENDIF (EXISTS "${__rule_filename_}")
        IF (NOT "${__old_rule_}" STREQUAL "${ARGN}")
            FILE(WRITE "${__rule_filename_}" "${ARGN}")
        ENDIF (NOT "${__old_rule_}" STREQUAL "${ARGN}")

        IF ("${ARGN}" MATCHES ";DEPENDS;")
            STRING(REPLACE ";DEPENDS;" ";DEPENDS;${__rule_filename_};" __new_cmd_ "${ARGN}")
        ELSE ("${ARGN}" MATCHES ";DEPENDS;")
            SET(__new_cmd_ "${ARGN};DEPENDS;${__rule_filename_}")
        ENDIF ("${ARGN}" MATCHES ";DEPENDS;")

        DEBUGMESSAGE (4 "${CURDIR}: ADD_CUSTOM_COMMAND[${__new_cmd_}]")
        ADD_CUSTOM_COMMAND(
            OUTPUT ${__rule_filename_}
            COMMAND echo "\"Somebody removed this file after cmake's run\"" >${__rule_filename_}
            COMMENT "Buildrule checkfile[${__rule_filename_}] is lost. Restoring."
        )
    ENDIF (__outfile_found_)

    ADD_CUSTOM_COMMAND(${__new_cmd_})
ENDMACRO (ADD_CUSTOM_COMMAND_EX)


# =============================================================================
# PREPARE_FLAGS
#  this macro resets all should-be-local variables that could be used by dtmk.cmake or user
#
MACRO (PREPARE_FLAGS)
    IF (NOT FILE_IS_INCLUDED)
        SET(__local_vars_ SRCS SRCDIR PEERLIBS PEERDIR PEERLDADD PEERDEPENDS OBJADD OBJADDE THIRDPARTY_OBJADD ADDSRC ADDINCL TOOLDEPDIR PROG LIB
            CURTARGET_EXCLUDED CREATEPROG INSTALLEDNAME ROOM PUBROOM # SUBDIR PEERDIR TOOLDIR LOCAL_WEAKNAME_LIB LOCAL_EXCLUDE_FROM_ALL
            TARGET_PATH LOCAL_EXECUTABLE_SUFFIX LOCAL_SHARED_LIBRARY_PREFIX LOCAL_SHARED_LIBRARY_SUFFIX
            CFLAGS CFLAGS_DEBUG CFLAGS_RELEASE CFLAGS_PROFILE CFLAGS_COVERAGE
            OBJDIRPREFIX OPTIMIZE NOOPTIMIZE CONLYFLAGS CXXFLAGS NO_COMPILER_WARNINGS NO_OPTIMIZE
            OPTIMIZE NOOPTIMIZE DTMK_D DTMK_I DTMK_F DTMK_L DTMK_CFLAGS DTMK_LIBTYPE DTMK_CXXFLAGS DTMK_CLEANFILES
            WERROR MAKE_ONLY_SHARED_LIB MAKE_ONLY_STATIC_LIB NO_SHARED_LIB NO_STATIC_LIB PROFFLAG USE_MPROF USEMPROF USE_THREADS USE_LIBBIND
            USE_MEMGUARD USEMEMGUARD USE_BERKELEY USE_GNUTLS USE_BOOST USE_LUA USE_PYTHON USE_PERL LINK_STATIC_PERL
            NOUTIL ALLOCATOR NOCHECK NOPEER SHLIB_MINOR SHLIB_MAJOR BUILD_WHEN_VARS NOBUILD_WHEN_VARS THISPROJECTDEPENDS LOCAL_MAKEFILE_CFG
            UT_DEPENDS IDE_FOLDER
            ${ADD_LOCALVARS})
        FOREACH(__item_ ${__local_vars_})
            SET(${__item_})
        ENDFOREACH(__item_)
    ENDIF (NOT FILE_IS_INCLUDED)
ENDMACRO (PREPARE_FLAGS)

MACRO (META_PROJECT prjname)
    INIT_CONFIG()

    STRING(REGEX MATCH "PROG" __prjnamePROG_ ${prjname})
    STRING(REGEX MATCH "LIB" __prjnameLIB_ ${prjname})

    IF (NOT "X${__prjnamePROG_}${__prjnameLIB_}X" MATCHES "XX")
        SET_IF_NOTSET(${prjname} ${ARGN})
        PROJECT_EX(${ARGN})
    ELSE (NOT "X${__prjnamePROG_}${__prjnameLIB_}X" MATCHES "XX")
        PROJECT_EX(${prjname})
    ENDIF (NOT "X${__prjnamePROG_}${__prjnameLIB_}X" MATCHES "XX")
    SET_APPEND(SRCDIR ${CMAKE_CURRENT_SOURCE_DIR})
ENDMACRO (META_PROJECT)

# =============================================================================
# PROJECT_EX2(prjname)
#
#
MACRO (PROJECT_EX2)
    PREPARE_FLAGS()
    META_PROJECT(${ARGN})
ENDMACRO (PROJECT_EX2)

MACRO (GET_GLOBAL_DIRNAME varname path)
    SET(__vartmppath_ "${path}")
#    GET_FILENAME_COMPONENT(__vartmppath_ ${__vartmppath_} PATH)
    STRING(REGEX REPLACE "^./" "" __vartmppath_ "${__vartmppath_}")
    STRING(REGEX REPLACE "/" "_" __vartmppath_ "${__vartmppath_}")
    STRING(REGEX REPLACE ":" "_" __vartmppath_ "${__vartmppath_}")
    STRING(REGEX REPLACE "\\." "_" __vartmppath_ "${__vartmppath_}")
    STRING(REGEX REPLACE "-" "_" ${varname} "${__vartmppath_}")
ENDMACRO (GET_GLOBAL_DIRNAME varname path)

MACRO(GETTARGETNAME varname)
    IF (NOT "X${ARGN}X" STREQUAL "XX")
        SET(__prjname_ "${ARGV1}")
    ELSE (NOT "X${ARGN}X" STREQUAL "XX")
        SET(__prjname_ "${LASTPRJNAME}")
    ENDIF (NOT "X${ARGN}X" STREQUAL "XX")
    GET_TARGET_PROPERTY(${varname} ${__prjname_} LOCATION)
ENDMACRO(GETTARGETNAME varname prjname)

# =============================================================================
# PROJECT_EX(prjname)
#  just a proxy
#
MACRO (PROJECT_EX prjname)
    SET_APPEND(PROJECT_DEFINED ${prjname})
    PROJECT(${prjname} ${ARGN})
    SET(CURDIR ${CMAKE_CURRENT_SOURCE_DIR})
    SET(BINDIR ${CMAKE_CURRENT_BINARY_DIR})
#    DEFAULT(TARGET_PATH "${CMAKE_CURRENT_SOURCE_DIR}")
    SET(TARGET_PATH "${CMAKE_CURRENT_SOURCE_DIR}")

    GET_GLOBAL_DIRNAME(GLOBAL_TARGET_NAME "${TARGET_PATH}")
ENDMACRO (PROJECT_EX)

MACRO (OWNER name)
    IF (USE_OWNERS)
        FILE(RELATIVE_PATH rp ${ARCADIA_ROOT} ${CMAKE_CURRENT_SOURCE_DIR})
        FILE (APPEND ${PROCESSED_OWNERS_FILE} "${rp}\t${name}\n")
        SET(rp)
    ENDIF (USE_OWNERS)
ENDMACRO (OWNER)

MACRO (UNIT type)
    SET(q ${ARGN})
    LIST(LENGTH q len)

    IF (len)
        LIST(GET q 0 name)
    ELSE (len)
        FILE(RELATIVE_PATH path "${ARCADIA_ROOT}" "${CMAKE_CURRENT_SOURCE_DIR}")
        STRING(REGEX REPLACE ".*/" "" name ${path})
        SET(path)
    ENDIF (len)

    ENTER_PROJECT()

    PROJECT_EX2(${type} ${name})
    SET(SAVEDNAME ${name})
    SET(LASTPRJNAME ${name})
    SET(SAVEDTYPE ${type})

    SET(q)
    SET(name)
    SET(len)
ENDMACRO (UNIT)

MACRO (PROGRAM)
    UNIT(PROG ${ARGN})
    SET(REALPRJNAME "${SAVEDNAME}")
    SET(CURPROJTYPES PROGRAM ${CURPROJTYPES})
ENDMACRO (PROGRAM)

MACRO (LIBRARY_BASE)
    UNIT(LIB ${ARGN})
    SET(REALPRJNAME "${SAVEDNAME}")
    SET(CURPROJTYPES LIBRARY ${CURPROJTYPES})
ENDMACRO (LIBRARY_BASE)

MACRO (LIBRARY)
    LIBRARY_BASE(${ARGN})

    SET(q ${ARGN})
    LIST(LENGTH q len)

    IF (NOT len)
        GET_FILENAME_COMPONENT(__lib_prefix_ ${CMAKE_CURRENT_SOURCE_DIR} PATH)
        GET_FILENAME_COMPONENT(__lib_prefix_ ${__lib_prefix_} NAME)
        SET(REALPRJNAME "${__lib_prefix_}-${SAVEDNAME}")
        SET(${SAVEDNAME}_USE_EX1 yes)
    ENDIF (NOT len)

    SET(q)
    SET(len)
ENDMACRO (LIBRARY)

MACRO (METALIBRARY)
    SET(CURPROJTYPES METALIBRARY ${CURPROJTYPES})
    DEBUGMESSAGE(3 "METALIBRARY. ARGN[${ARGN}]")
# Do not uncomment the following line
#    UNIT(METALIB ${ARGN})
ENDMACRO (METALIBRARY)

MACRO (END)
    LIST(GET CURPROJTYPES 0 __curtype_)
    LIST(REMOVE_AT CURPROJTYPES 0)
    IF (__curtype_ STREQUAL "METALIBRARY")
        DEBUGMESSAGE(3 "METALIBRARY end @ ${CURDIR}, SAVEDNAME[${SAVEDNAME}]")
    ELSE (__curtype_ STREQUAL "METALIBRARY")
        IF (SAVEDNAME)
            SET(__local_exclude_from_all_)
            IF (LOCAL_EXCLUDE_FROM_ALL)
                SET(__local_exclude_from_all_ EXCLUDE_FROM_ALL)
            ENDIF (LOCAL_EXCLUDE_FROM_ALL)
            IF (${SAVEDTYPE} STREQUAL LIB)
                IF (${SAVEDNAME}_USE_EX1)
                    ADD_LIBRARY_EX1(${SAVEDNAME} ${__local_exclude_from_all_} ${SRCS})
                ELSE (${SAVEDNAME}_USE_EX1)
                    ADD_LIBRARY_EX(${SAVEDNAME} ${__local_exclude_from_all_} ${SRCS})
                ENDIF (${SAVEDNAME}_USE_EX1)
            ELSE (${SAVEDTYPE} STREQUAL LIB)
                ADD_EXECUTABLE_EX(${SAVEDNAME} ${__local_exclude_from_all_} ${SRCS})
            ENDIF (${SAVEDTYPE} STREQUAL LIB)
        ENDIF (SAVEDNAME)

        SET(SAVEDNAME)
        SET(SAVEDTYPE)

        LEAVE_PROJECT()
    ENDIF (__curtype_ STREQUAL "METALIBRARY")
ENDMACRO (END)

# =============================================================================
# INSTALLTO
#

MACRO (INSTALLTO)
    SET(q ${ARGN})
    LIST(LENGTH q len)

    IF (len)
        LIST(GET q 0 bindir)
    ELSE (len)
        SET(bindir "bin")
    ENDIF (len)

    SET(q)
    SET(len)

    SET(INSTALLEDNAME ${PROG})
    SET(INSTALLDIR "${INSTALLBASE}/${bindir}")
    SET(bindir)
ENDMACRO (INSTALLTO)

# =============================================================================
# FIND_FILE_IN_DIRS
#
MACRO (FIND_FILE_IN_DIRS found_var filename_var srcdir_var)
    SET(__item_fullname_ no)
    FOREACH(__diritem_ ${${srcdir_var}})
        DEBUGMESSAGE(3 "-- trying ${__diritem_}/${__item_}")
        GET_FILENAME_COMPONENT(__item_absfullname_ ${__diritem_}/${${filename_var}} ABSOLUTE)

        IF (NOT __item_fullname_)
            SET_APPEND(SRCFILE_TRIEDNAMES ${__item_absfullname_})
            IF (EXISTS "${__item_absfullname_}")
                SET(__item_fullname_ ${__item_absfullname_})
            ENDIF (EXISTS "${__item_absfullname_}")
        ELSE (NOT __item_fullname_)
        ENDIF (NOT __item_fullname_)
    ENDFOREACH(__diritem_)

    IF (__item_fullname_)
        DEBUGMESSAGE(3 "--- is ${__item_fullname_}")
        SET(${filename_var} ${__item_fullname_})
        SET(${found_var} yes)
    ELSE (__item_fullname_)
        DEBUGMESSAGE(2 "${CMAKE_CURRENT_SOURCE_DIR}: Source file not exists [${__item_}]. Generated?")
        DEBUGMESSAGE(2 "\tTried names [${SRCFILE_TRIEDNAMES}]")
        SET(${found_var} no)
    ENDIF (__item_fullname_)
ENDMACRO (FIND_FILE_IN_DIRS)

# =============================================================================
# IS_COMPILABLE_TYPE
# This macro checks if this file could be built by CMake itself
#
MACRO(IS_COMPILABLE_TYPE res_var file_ext)
    SET(${res_var} no)

    # TODO: use ${CMAKE_CXX_SOURCE_FILE_EXTENSIONS} ${CMAKE_C_SOURCE_FILE_EXTENSIONS}
    # Note that CXX_... contains c++ (invalid regular expression)

    FOREACH(__cpp_ext_ ${COMPILABLE_FILETYPES})
        IF (NOT ${res_var})
            # this doesn't work because file_exc is the longest extension (.byk.cpp, for example):
            IF ("${file_ext}" STREQUAL ".${__cpp_ext_}")
                SET(${res_var} yes)
            ENDIF ("${file_ext}" STREQUAL ".${__cpp_ext_}")
        ENDIF (NOT ${res_var})
    ENDFOREACH(__cpp_ext_)
ENDMACRO(IS_COMPILABLE_TYPE res_var file_ext)

# =============================================================================
# SPLIT_FILENAME
#   This macro exists because "The longest file extension is always considered"
#     in GET_FILENAME_COMPONENT macro. We do need the longest name and shortest
#     file extension.
#
MACRO (SPLIT_FILENAME namewe_var ext_var)
    # Get name and extention
    SET(__file_name_ "${ARGN}")
    STRING(REGEX REPLACE ".*\\." "." ${ext_var} "${ARGN}")
    STRING(REGEX REPLACE "\\.[^.]+$" "" ${namewe_var} "${ARGN}")
    DEBUGMESSAGE(3 "Splitted [${ARGN}] to [${${namewe_var}}][${${ext_var}}]")
ENDMACRO (SPLIT_FILENAME)

# =============================================================================
# TRY_PREPARE_SRC_BUILDRULE
#
MACRO (TRY_PREPARE_SRC_BUILDRULE filename_var srcdir_var)
    GET_FILENAME_COMPONENT(__file_name_ "${${filename_var}}" NAME)
    GET_FILENAME_COMPONENT(__file_path_ "${${filename_var}}" PATH)

    SPLIT_FILENAME(__file_namewe_ __file_ext_ ${__file_name_})
    IS_COMPILABLE_TYPE(__compilable_ ${__file_ext_})

    IF (__compilable_)
        SET(__srcfound_ no)
        FOREACH(__new_ext_ ${SUFFIXES})
            IF (NOT __srcfound_)
                SET(__filename_test_ "${__file_path_}/${__file_namewe_}.${__new_ext_}")
                FIND_FILE_IN_DIRS(__srcfound_ __filename_test_ "${srcdir_var}")

                IF (__srcfound_)
                    # If ${filename_var} is a relative path then add a BINARY_DIR-prefix
                    IF (NOT "${${filename_var}}" MATCHES "^/")
                        SET(${filename_var} "${CMAKE_CURRENT_BINARY_DIR}/${${filename_var}}")
                    ENDIF (NOT "${${filename_var}}" MATCHES "^/")

                    ADD_SRC_BUILDRULE(.${__new_ext_} ${__filename_test_} ${filename_var})
                ENDIF (__srcfound_)
            ENDIF (NOT __srcfound_)
        ENDFOREACH(__new_ext_)
        IF (NOT __srcfound_)
            DEBUGMESSAGE(1 "Can't find a source file to build ${${filename_var}} (@ ${CMAKE_CURRENT_SOURCE_DIR})")
        ENDIF (NOT __srcfound_)
    ELSE (__compilable_)
        # This file is not cpp AND not exists.
        # Probably is it the case when multiple suffixes should be applied.
        # TODO: Process this case
    ENDIF (__compilable_)
ENDMACRO(TRY_PREPARE_SRC_BUILDRULE)


# =============================================================================
# PREPARE_SRC_BUILDRULE
#
MACRO(PREPARE_SRC_BUILDRULE srcfile_var)
    GET_FILENAME_COMPONENT(__file_name_ "${${srcfile_var}}" NAME)
    GET_FILENAME_COMPONENT(__file_path_ "${${srcfile_var}}" PATH)

    DEBUGMESSAGE(3 "-- ${srcfile_var} = ${${srcfile_var}} (${__file_path_} / ${__file_namewe_} . ${__file_ext_})")
    SPLIT_FILENAME(__file_namewe_ __file_ext_ ${__file_name_})

    IS_COMPILABLE_TYPE(__compilable_ ${__file_ext_})
    IF (__compilable_)
        # Nothing to do
        DEBUGMESSAGE(2 "----------- ${${srcfile_var}} is buildable type (extension is [${__file_ext_}])")
        GET_GLOBAL_DIRNAME(__item_global_ "${${srcfile_var}}")
        IF (${__item_global_}_PROPS)
            # Just to apply ..._PROPS
            ADD_SRC_BUILDRULE(".cpp" "${${srcfile_var}}" ${srcfile_var} ${ARGN})
        ENDIF (${__item_global_}_PROPS)
    ELSE (__compilable_)
        # Check if this file extension is in SUFFIXES
        SET(__suffix_found_ no)
        DEBUGMESSAGE(2 "----------- ${${srcfile_var}} is not c/cpp")
        FOREACH(__new_ext_ ${SUFFIXES})
            IF ("${__file_ext_}" MATCHES "^.${__new_ext_}$")
                SET(__orig_filename_ "${${srcfile_var}}")
                SET(${srcfile_var} "${CMAKE_CURRENT_BINARY_DIR}/${__file_namewe_}.cpp")
                ADD_SRC_BUILDRULE(${__file_ext_} "${__orig_filename_}" ${srcfile_var} ${ARGN})
                SET(__suffix_found_ yes)
            ENDIF ("${__file_ext_}" MATCHES "^.${__new_ext_}$")
        ENDFOREACH(__new_ext_)
        IF (NOT __suffix_found_)
            SET(${srcfile_var} ${${srcfile_var}}.rule-not-found.cpp)
        ENDIF (NOT __suffix_found_)
    ENDIF (__compilable_)
ENDMACRO(PREPARE_SRC_BUILDRULE)

MACRO(IS_FULLNAME filename var)
    SET(${var} no)
    IF (NOT WIN32 AND "${filename}" MATCHES "^/")
        SET(${var} yes)
    ELSEIF (WIN32 AND "${filename}" MATCHES "^.:")
        SET(${var} yes)
    ENDIF (NOT WIN32 AND "${filename}" MATCHES "^/")
ENDMACRO(IS_FULLNAME)


#
# The source barrier will cause compilation of any file to start only after all
# source files have been updated. Use sources barrier when some of the source
# files are generated and are used by each other or non-generated files; for
# instance, when using .proto files when they can import each other or in the
# same directory as non-generated source files which might be using them.
#

MACRO (SET_NEED_SRC_BARRIER)
    SET_PROPERTY(DIRECTORY PROPERTY NEED_BARRIER_SOURCES TRUE)
ENDMACRO ()

MACRO (GET_NEED_SRC_BARRIER var)
    GET_PROPERTY(${var} DIRECTORY PROPERTY NEED_BARRIER_SOURCES)
ENDMACRO ()


#
# Helper macros to propagate dependencies on directories containing generated
# files (mainly headers). The result will be used in dtmk.cmake's PEERDIR
# processing, when all peer libraries are known and can be checked for having
# generated files themselves or, in turn, depending on other such libraries.
#

MACRO (SET_DIR_HAS_GENERATED)
    SET_PROPERTY(DIRECTORY PROPERTY DIR_HAS_GENERATED TRUE)
    SET_NEED_SRC_BARRIER()
ENDMACRO ()

MACRO (GET_DIR_HAS_GENERATED var dir)
    GET_PROPERTY(${var} DIRECTORY ${dir} PROPERTY DIR_HAS_GENERATED)
ENDMACRO ()

MACRO (ADD_DIR_DEP_GENERATED)
    SET_PROPERTY(DIRECTORY APPEND PROPERTY DIR_DEP_GENERATED ${ARGN})
ENDMACRO ()

FUNCTION (GET_DIR_DEP_GENERATED var)
    SET(__nodirs TRUE) # LIST(LENGTH) didn't work on ARGN for me
    FOREACH (__dir ${ARGN})
        SET(__nodirs FALSE)
        GET_PROPERTY(__val DIRECTORY ${__dir} PROPERTY DIR_DEP_GENERATED)
        IF (DEFINED __val)
            SET_APPEND(__set ${__val})
        ENDIF ()
    ENDFOREACH ()
    IF (__nodirs)
        GET_PROPERTY(__set DIRECTORY PROPERTY DIR_DEP_GENERATED)
    ENDIF ()
    IF (DEFINED __set)
        LIST(REMOVE_DUPLICATES __set)
        SET(${var} ${__set} PARENT_SCOPE)
    ENDIF ()
ENDFUNCTION ()

FUNCTION (IMP_DIR_DEP_GENERATED dir)
    GET_DIR_DEP_GENERATED(__dir_dep_generated ${dir} ${ARGN})
    IF (DEFINED __dir_dep_generated)
        ADD_DIR_DEP_GENERATED(${__dir_dep_generated})
    ENDIF ()
ENDFUNCTION ()



# =============================================================================
# PREPARE_SRC_FILES
#
MACRO (PREPARE_SRC_FILES srcs srcdir srcdirbase)
    SET(__new_srcs "")
    DEBUGMESSAGE(2 "srcdirbase[${srcdirbase}]; SRCDIR[${${srcdir}}] @ ${CMAKE_CURRENT_SOURCE_DIR}")
    IF (${srcdir})
        # Prepare full directory names
        SET(__srcdir_full_)
        FOREACH(__diritem_ ${${srcdir}})
            IS_FULLNAME("${__diritem_}" __is_full_)

            IF (__is_full_)
                SET_APPEND(__srcdir_full_ ${__diritem_})
            ELSEIF ("${__diritem_}" MATCHES "^.$")
                SET_APPEND(__srcdir_full_ ${CMAKE_CURRENT_SOURCE_DIR})
            ELSE (__is_full_)
                SET_APPEND(__srcdir_full_ ${srcdirbase}/${__diritem_})
            ENDIF (__is_full_)
        ENDFOREACH(__diritem_)

        FOREACH(__item_ ${${srcs}})
            SET(SRCFILE_TRIEDNAMES)
            SET(__filename_ ${__item_})

            IF (NOT EXISTS ${__item_})
                IF ("${__item_}" MATCHES "^/")
                    # If filename is full path then add it as is: probably the custom build rule is already added
                    SET_APPEND(__new_srcs ${__item_})
                ELSE ("${__item_}" MATCHES "^/")


                    FIND_FILE_IN_DIRS(__found_ __filename_ __srcdir_full_)

                    IF (__found_)
                        # Found file with given name. Probably, not compilable by c/c++ compiler.
                        PREPARE_SRC_BUILDRULE(__filename_ ${__item_})
                        SET_APPEND(__new_srcs ${__filename_})
                    ELSE (__found_)
                        # File has not been found, trying to find source file from gperf, l, rl, etc.
                        DEBUGMESSAGE(2 "----------- ${__item_} not found")
                        TRY_PREPARE_SRC_BUILDRULE(__filename_ __srcdir_full_)
                        SET_APPEND(__new_srcs ${__filename_})
                    ENDIF (__found_)
                ENDIF ("${__item_}" MATCHES "^/")
            ELSE (NOT EXISTS ${__item_})
                # File with given name exists. Probably, not compilable by c/c++ compiler.
                PREPARE_SRC_BUILDRULE(__filename_)
                SET_APPEND(__new_srcs ${__filename_})
        #       MESSAGE("---- is ${__item_}")
            ENDIF (NOT EXISTS ${__item_})
        ENDFOREACH(__item_)

        GET_NEED_SRC_BARRIER(__need_src_barrier)
        IF (__need_src_barrier)
            SET(__name_src_barrier _barrier_sources)
            SET(__path_src_barrier ${BINDIR}/${__name_src_barrier})
            FOREACH (__item_ ${__new_srcs})
                SET_PROPERTY(SOURCE ${__item_}
                    APPEND PROPERTY OBJECT_DEPENDS ${__path_src_barrier})
            ENDFOREACH ()
            IF (WIN32)
                SET(__touch_cmd ${ARCADIA_ROOT}/cmake/include/touch.py)
            ELSE ()
                SET(__touch_cmd touch)
            ENDIF ()
            # we need the file to exist, not be recent; if it doesn't exist or
            # has a fresh mtime after this rule, all objects would be recompiled
            ADD_CUSTOM_COMMAND(
                OUTPUT    ${__path_src_barrier}
                COMMAND   ${__touch_cmd} -t 200001010000 ${__name_src_barrier}
                DEPENDS   ${__new_srcs}
                WORKING_DIRECTORY ${BINDIR}
                COMMENT   ""
            )
        ENDIF ()

        SET(${srcs} ${__new_srcs})
        SET(__headers_)
        IF (WIN32 AND NOT NO_ADD_HEADERS)
            FOREACH(__item_ ${${srcs}})
                IS_FULLNAME(${__item_} __if_full_)
                IF (__if_full_)
                    IF (${__item_} MATCHES ".cpp$")
                        STRING(REPLACE ".cpp" ".h" __h_file_ ${__item_})
                    ELSEIF (${__item_} MATCHES ".c$")
                        STRING(REPLACE ".c" ".h" __h_file_ ${__item_})
                    ELSEIF (${__item_} MATCHES ".cc$")
                        STRING(REPLACE ".cc" ".h" __h_file_ ${__item_})
                    ENDIF (${__item_} MATCHES ".cpp$")
                    IF (EXISTS ${__h_file_})
                        SET_APPEND(__headers_ ${__h_file_})
                    ENDIF (EXISTS ${__h_file_})
                ENDIF (__if_full_)
            ENDFOREACH(__item_ ${${srcs}})
            SET_APPEND(${srcs} ${__headers_})
            DEBUGMESSAGE(2 "Headers added to project [" ${__headers_} "]")
        ENDIF (WIN32 AND NOT NO_ADD_HEADERS)
    ENDIF (${srcdir})
ENDMACRO (PREPARE_SRC_FILES)

# =============================================================================
# SAVE_VARIABLES. Saves variables. Use CHECK_SAVED_VARIABLES to check if they
#   have changed
#
MACRO (SAVE_VARIABLES)
    FOREACH(__item_ ${ARGN})
        SET(__saved_${__item_} "${${__item_}}")
    ENDFOREACH(__item_ ${ARGN})
ENDMACRO (SAVE_VARIABLES)

MACRO (CHECK_SAVED_VARIABLES outvar)
    SET(${outvar} "")
    FOREACH(__item_ ${ARGN})
        IF (NOT "${__saved_${__item_}}" STREQUAL "${${__item_}}")
            SET_APPEND(${outvar} ${__item_})
        ENDIF (NOT "${__saved_${__item_}}" STREQUAL "${${__item_}}")
    ENDFOREACH(__item_ ${ARGN})
ENDMACRO (CHECK_SAVED_VARIABLES)

# =============================================================================
# SET_AS_LOCAL. Invoke it before PROJECT_EX
#
MACRO(SET_AS_LOCAL)
    SET_APPEND(ADD_LOCALVARS ${ARGN})
ENDMACRO(SET_AS_LOCAL)

# =============================================================================
# LIST_REMOVE_DUPLICATES(listname)
#
MACRO (LIST_REMOVE_DUPLICATES listname)
    SET(__newlist_)
    SET(__remove_only_ ".*")
    IF (NOT "X${ARGN}X" STREQUAL "XX")
        SET(__remove_only_ "${ARGV1}")
        DEBUGMESSAGE(2 "Removing only '${__remove_only_}'")
    ENDIF (NOT "X${ARGN}X" STREQUAL "XX")
    WHILE (${listname})
        LIST(GET ${listname} 0 __head_)
        LIST(APPEND __newlist_ ${__head_})
        IF ("${__head_}" MATCHES "${__remove_only_}")
            LIST(REMOVE_ITEM ${listname} ${__head_})
            DEBUGMESSAGE(2 "Removing: from ${listname} '${__head_}'")
        ELSE ("${__head_}" MATCHES "${__remove_only_}")
            LIST(REMOVE_AT ${listname} 0)
            DEBUGMESSAGE(2 "Removing: from ${listname} 0'th element ('${__head_}')")
        ENDIF ("${__head_}" MATCHES "${__remove_only_}")
    ENDWHILE (${listname})
    SET(${listname} ${__newlist_})
ENDMACRO (LIST_REMOVE_DUPLICATES)

# =============================================================================
# Internal Macros to apply collected DTMK_D, DTMK_I etc. to srcs
#
MACRO(_APPLY_DEFS srcs)
    SET(__sources_ ${srcs} ${ARGN})
    SEPARATE_ARGUMENTS(__sources_)
    SET(__LOCAL_DTMK_I)
    INCLUDE_DIRECTORIES(${DTMK_I})
    DEBUGMESSAGE(2 "${CMAKE_CURRENT_SOURCE_DIR} DTMK_I=${DTMK_I}\n")

#   FOREACH(__item_ ${DTMK_I})
#       IF (WIN32)
#           SET_APPEND(__LOCAL_DTMK_I "/I \"${__item_}\"")
#       ELSE (WIN32)
#           SET_APPEND(__LOCAL_DTMK_I -I${__item_})
#       ENDIF (WIN32)
#   ENDFOREACH(__item_)

    ADD_DEFINITIONS(${DTMK_D})
    DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: DTMK_D=${DTMK_D}\n")
#   SET(__LOCAL_COMPILE_FLAGS ${DTMK_CFLAGS} ${DTMK_CXXFLAGS}) #${__LOCAL_DTMK_I}
#   SEPARATE_ARGUMENTS_SPACE(__LOCAL_COMPILE_FLAGS)

    DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: __LOCAL_COMPILE_FLAGS=${__LOCAL_COMPILE_FLAGS}\n__sources_=${__sources_}\n")

IF (USE_PRECOMPILED)
    SET(__have_stdafxcpp_)
    STRING(TOLOWER "${__sources_}" srclist)
    IF ("${srclist}" MATCHES "stdafx.cpp")
        ENABLE(__have_stdafxcpp_)
    ENDIF ("${srclist}" MATCHES "stdafx.cpp")

    IF (WIN32 AND __have_stdafxcpp_)
        FOREACH(__item_ ${__sources_})
            STRING(TOLOWER "${__item_}" __name_lwrcase_)
            IF (${__name_lwrcase_} MATCHES "stdafx.cpp\$")
#                MESSAGE("ApplyDefs: ${__item_}")
                SET_SOURCE_FILES_PROPERTIES(${__item_} PROPERTIES COMPILE_FLAGS "/Yc\"stdafx.h\" /Fp\"${BINDIR}/${CMAKE_CFG_INTDIR}/${PROJECTNAME}.pch\"")
            ELSEIF (${__name_lwrcase_} MATCHES ".*h[p]*\$")
                # It's not good way to compile headers
            ELSE (${__name_lwrcase_} MATCHES "stdafx.cpp\$")
                SET_SOURCE_FILES_PROPERTIES(${__item_} PROPERTIES COMPILE_FLAGS "/Yu\"stdafx.h\" /Fp\"${BINDIR}/${CMAKE_CFG_INTDIR}/${PROJECTNAME}.pch\"")
            ENDIF (${__name_lwrcase_} MATCHES "stdafx.cpp\$")
        ENDFOREACH(__item_)
    ENDIF (WIN32 AND __have_stdafxcpp_)
ENDIF (USE_PRECOMPILED)

ENDMACRO(_APPLY_DEFS)

# =============================================================================
# lorder_for_peerlibs
#   (internal macro)
MACRO(lorder_for_peerlibs prjname peerlibs_var peerdepends_var sources_var)
    IF (PEERDEPENDS)
        IF (USE_LORDER)
            ADD_CUSTOM_COMMAND(
                OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp
                COMMAND echo "${prjname}: current libraries order:" > ${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp
                COMMAND echo ${${peerlibs_var}} >> ${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp
                COMMAND echo "Good libraries order:" && lorder ${${peerlibs_var}} | tsort 2>/dev/null 1>${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp || true
                COMMAND cat ${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp
                COMMAND ${RM} ${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp
                COMMAND cp ${ARCADIA_ROOT}/cmake/include/_cmake_fake_src.cpp ${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp
                DEPENDS ${${peerdepends_var}}
                COMMENT "lorder+tsort for peerlibs of ${prjname}"
            )
            LIST(APPEND ${sources_var} ${CMAKE_CURRENT_BINARY_DIR}/_cmake_fake_src.cpp)
        ENDIF (USE_LORDER)
    ENDIF (PEERDEPENDS)
ENDMACRO(lorder_for_peerlibs)

MACRO(STRIP_KEYWORDS src_var kw_var)
    SET(${kw_var})
    SET(${src_var} ${ARGN})
    FOREACH(__i_ RANGE 2)
        LIST(GET ${src_var} 0 __item_)
        STRING(TOUPPER "${__item_}" __item_)
        IF ("${__item_}" MATCHES "EXCLUDE_FROM_ALL")
            SET_APPEND(${kw_var} EXCLUDE_FROM_ALL)
            LIST(REMOVE_AT ${src_var} 0)
        ENDIF ("${__item_}" MATCHES "EXCLUDE_FROM_ALL")
        IF ("${__item_}" MATCHES "WIN32")
            SET_APPEND(${kw_var} WIN32)
            LIST(REMOVE_AT ${src_var} 0)
        ENDIF ("${__item_}" MATCHES "WIN32")
    ENDFOREACH(__i_)

#   IF (${kw_var})
#       MESSAGE("${CMAKE_CURRENT_SOURCE_DIR}: kw[${${kw_var}}], srcs[${${src_var}}]")
#   ENDIF (${kw_var})
ENDMACRO(STRIP_KEYWORDS)

MACRO (ON_TARGET_FINISH)
    IF (DTMK_CLEANFILES)
        SET_DIRECTORY_PROPERTIES(PROPERTIES
            ADDITIONAL_MAKE_CLEAN_FILES "${DTMK_CLEANFILES}"
        )
    ENDIF (DTMK_CLEANFILES)
    IF (COMMAND ON_TARGET_FINISH_HOOK)
        ON_TARGET_FINISH_HOOK()
    ENDIF (COMMAND ON_TARGET_FINISH_HOOK)
ENDMACRO (ON_TARGET_FINISH)

MACRO (CHECK_PROJECT_DEFINED prjname prjtype)
    IF (NOCHECK)
        NOBUILD(MAKE_CHECK)
    ENDIF (NOCHECK)

    SET(__nobuild_)
    LIST(LENGTH PROJECT_DEFINED __len_)
    IF (NOT PROJECT_DEFINED)
        MESSAGE(STATUS "buildrules.cmake warning: ADD_${prjtype}_EX without corresponding PROJECT_EX(2) in ${CURDIR}. Project[${prjname}]")
        SET(__nobuild_ yes)
    ELSEIF (NOT __len_ EQUAL 1)
        MESSAGE(STATUS "buildrules.cmake warning: nested PROJECT_EX(2) in ${CURDIR}. Projects[${PROJECT_DEFINED}]")
        SET(__nobuild_ yes)
    ENDIF (NOT PROJECT_DEFINED)
    CDR(PROJECT_DEFINED ${PROJECT_DEFINED})

    IF (__nobuild_)
        MESSAGE(STATUS "buildrules.cmake warning: ADD_${prjtype}_EX, target [${prjname}] not created")
    ENDIF (__nobuild_)

    SET(__excluded_)
    SET(__excl_reason_)
    IF (NOT "X${BUILD_WHEN_VARS}X" STREQUAL "XX")
        SET(__excluded_ yes)
        SET(__excl_reason_ "BUILD(${BUILD_WHEN_VARS})")
        FOREACH(__item_ ${BUILD_WHEN_VARS})
            IF (${__item_})
                SET(__excluded_ no)
                SET(__excl_reason_)
            ENDIF (${__item_})
        ENDFOREACH(__item_)
    ENDIF (NOT "X${BUILD_WHEN_VARS}X" STREQUAL "XX")

    FOREACH(__item_ ${NOBUILD_WHEN_VARS})
        IF (${__item_})
            SET_APPEND(__excl_reason_ "NOBUILD(${__item_})")
            SET(__excluded_ yes)
        ENDIF (${__item_})
    ENDFOREACH(__item_)

    IF (__excluded_)
        FILE(APPEND "${EXCLTARGET_LIST_FILENAME}" "${prjname} ${CURDIR} ${__excl_reason_}\n")
        SET(__nobuild_ yes)
    ENDIF (__excluded_)

    SET(${prjname}_EXCLUDED ${__nobuild_})
    SET(CURTARGET_EXCLUDED ${__nobuild_})

ENDMACRO (CHECK_PROJECT_DEFINED)

MACRO (CHECK_DUPLICATE_TARGET prjname)
    GET_TARGET_PROPERTY(__target_location_ "${prjname}" LOCATION)
    GET_TARGET_PROPERTY(__target_type_ "${prjname}" TYPE)
    IF (__target_type_)
        MESSAGE(SEND_ERROR "Target \"${prjname}\" of type ${__target_type_} is already defined, its binary located at [${__target_location_}]")
    ENDIF (__target_type_)
ENDMACRO (CHECK_DUPLICATE_TARGET)

MACRO(SET_IDE_FOLDER target)
    SET_PROPERTY(GLOBAL PROPERTY USE_FOLDERS ON)
    IF (MSVC)
        IF (NOT ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 2.8)
            SET(q ${IDE_FOLDER})
            LIST(LENGTH q len)
            IF (len)
                SET_PROPERTY(TARGET "${target}" PROPERTY FOLDER "${IDE_FOLDER}")
            ELSE (len)
                FILE(RELATIVE_PATH rp ${ARCADIA_ROOT} ${CMAKE_CURRENT_SOURCE_DIR})
                STRING(REGEX REPLACE "(.*)/.*" "\\1" rp ${rp})
                SET_PROPERTY(TARGET "${target}" PROPERTY FOLDER "${rp}")
            ENDIF (len)
        ENDIF(NOT ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 2.8)
    ENDIF(MSVC)
ENDMACRO(SET_IDE_FOLDER)

# =============================================================================
# ADD_EXECUTABLE_EX(exename sources)
#
MACRO (ADD_EXECUTABLE_EX exename)
    CHECK_PROJECT_DEFINED(${exename} EXECUTABLE)
    CHECK_SRCS(${ARGN})
    IF (NOT __nobuild_)
        SET(PROJECTNAME ${exename})
        EXEC_DTMK()

        STRIP_KEYWORDS(__sources_1 __keywords_ ${ARGN})
#       SET(__sources_1 ${ARGN})
        SEPARATE_ARGUMENTS(__sources_1)
        IF (__sources_1)
            PREPARE_SRC_FILES(__sources_1 SRCDIR ${ARCADIA_ROOT})
            _APPLY_DEFS(${__sources_1})

            lorder_for_peerlibs(${exename} PEERLIBS PEERDEPENDS __sources_1)

            IF (THIRDPARTY_OBJADD)
                SEPARATE_ARGUMENTS_SPACE(THIRDPARTY_OBJADD)
                SET(CMAKE_CXX_LINK_EXECUTABLE "${CMAKE_CXX_LINK_EXECUTABLE} ${THIRDPARTY_OBJADD} ")
                SET(CMAKE_C_LINK_EXECUTABLE "${CMAKE_C_LINK_EXECUTABLE} ${THIRDPARTY_OBJADD} ")
            ENDIF (THIRDPARTY_OBJADD)

            SET(tmp_CMAKE_EXECUTABLE_SUFFIX ${CMAKE_EXECUTABLE_SUFFIX})
            IF (LOCAL_EXECUTABLE_SUFFIX)
                SET(CMAKE_EXECUTABLE_SUFFIX ${LOCAL_EXECUTABLE_SUFFIX})
            ENDIF (LOCAL_EXECUTABLE_SUFFIX)

            CHECK_DUPLICATE_TARGET(${exename})
            ADD_EXECUTABLE(${exename} ${__keywords_} ${__sources_1})
            SET_IDE_FOLDER(${exename})
            
#           IF (INSTALLEDNAME)
#               SET_TARGET_PROPERTIES(${exename} PROPERTIES
#                   OUTPUT_NAME ${INSTALLEDNAME})
#           ENDIF(INSTALLEDNAME)

            APPLY_PEERDIR_SUBDIR(${exename})
            GETTARGETNAME(__this_ ${exename})

#            IF (NOT ${GLOBAL_TARGET_NAME}_DEPENDNAME_PROG)
                SET(${GLOBAL_TARGET_NAME}_DEPENDNAME_PROG ${exename} CACHE INTERNAL "" FORCE)
                DEBUGMESSAGE(2 "${exename}: TOOLDIR in CURDIR will set, ${GLOBAL_TARGET_NAME}_DEPENDNAME_PROG is [${exename}]")
#            ELSE (NOT ${GLOBAL_TARGET_NAME}_DEPENDNAME_PROG)
#                DEBUGMESSAGE(2 "${exename}: TOOLDIR in CURDIR already set, ${GLOBAL_TARGET_NAME}_DEPENDNAME_PROG is[${${GLOBAL_TARGET_NAME}_DEPENDNAME_PROG}]")
#            ENDIF (NOT ${GLOBAL_TARGET_NAME}_DEPENDNAME_PROG)

            IF (CREATEPROG AND PLATFORM_SUPPORTS_SYMLINKS)
                IF ("${CURDIR}" STREQUAL "${BINDIR}")
                     # Do not make symlink to executable (issue related to per-project unittests)
                     ENABLE(__nolink_local_)
                ENDIF ("${CURDIR}" STREQUAL "${BINDIR}")
                IF (NOT NOLINK AND NOT __nolink_local_)
                    ADD_CUSTOM_COMMAND(TARGET ${exename}
                        POST_BUILD
                        COMMAND ln -sf ${__this_} ${CMAKE_CURRENT_SOURCE_DIR}/${CREATEPROG}${CMAKE_EXECUTABLE_SUFFIX}
                        COMMENT "Making a symbolic link from ${__this_} to ${CMAKE_CURRENT_SOURCE_DIR}/${CREATEPROG}${CMAKE_EXECUTABLE_SUFFIX}"
                        )
                    SET_APPEND(DTMK_CLEANFILES ${CMAKE_CURRENT_SOURCE_DIR}/${CREATEPROG}${CMAKE_EXECUTABLE_SUFFIX})
                ENDIF (NOT NOLINK AND NOT __nolink_local_)

                IF (EXESYMLINK_DIR)
                    ADD_CUSTOM_COMMAND(TARGET ${exename}
                        POST_BUILD
                        COMMAND mkdir -p "${EXESYMLINK_DIR}"
                        COMMAND ln -sf ${__this_} ${EXESYMLINK_DIR}/${CREATEPROG}${CMAKE_EXECUTABLE_SUFFIX}
                        COMMENT "Making a symbolic link from ${__this_} to ${EXESYMLINK_DIR}/${CREATEPROG}${CMAKE_EXECUTABLE_SUFFIX}"
                        )
                    SET_APPEND(DTMK_CLEANFILES ${EXESYMLINK_DIR}/${CREATEPROG}${CMAKE_EXECUTABLE_SUFFIX})
                ENDIF (EXESYMLINK_DIR)
            ENDIF (CREATEPROG AND PLATFORM_SUPPORTS_SYMLINKS)

            IF (INSTALLEDNAME)
                IF (NOT NOSTRIP AND NOT WIN32 AND "${CMAKE_BUILD_TYPE}" MATCHES "Release")

                    SET(__STRIP_ARG "${__this_}")
                    SET(__STRIP_ARG_STAT_DEBUGINFO "${__this_}.static.debuginfo")

                    ADD_CUSTOM_COMMAND(TARGET ${exename}
                        POST_BUILD
                        COMMAND cp ${__STRIP_ARG} ${__STRIP_ARG_STAT_DEBUGINFO}
                        COMMAND strip ${__STRIP_ARG}
                        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
                        VERBATIM
                        )

                    SET_APPEND(DTMK_CLEANFILES ${__STRIP_ARG_STAT_DEBUGINFO})
                ENDIF (NOT NOSTRIP AND NOT WIN32 AND "${CMAKE_BUILD_TYPE}" MATCHES "Release")

                IF (INSTALLDIR)
                    IF (WIN32)
                        SET_TARGET_PROPERTIES(${exename} PROPERTIES OUTPUT_NAME  ${INSTALLEDNAME})
                        INSTALL(
                            TARGETS ${exename}
                            DESTINATION ${INSTALLDIR}
                        )
                    ELSE (WIN32)
                        STRING(TOUPPER "${CMAKE_BUILD_TYPE}" __bt_)
                        GET_TARGET_PROPERTY(__target_location_ ${exename} ${__bt_}_LOCATION)
                        DEBUGMESSAGE(3 "${__target_location_} ${exename} LOCATION")
                        INSTALL(PROGRAMS ${__target_location_}
                            DESTINATION ${INSTALLDIR}
                            RENAME ${INSTALLEDNAME}${CMAKE_EXECUTABLE_SUFFIX}
                        )
                    ENDIF (WIN32)
                ENDIF (INSTALLDIR)

            ENDIF (INSTALLEDNAME)

            SET(CMAKE_EXECUTABLE_SUFFIX ${tmp_CMAKE_EXECUTABLE_SUFFIX})

            IF (NOT "X${TARGET_SHLIB_MAJOR}${TARGET_SHLIB_MINOR}X" MATCHES "XX")
                SET_TARGET_PROPERTIES(${exename} PROPERTIES
                    VERSION "${TARGET_SHLIB_MAJOR}" #.${TARGET_SHLIB_MINOR}"
                )
            ENDIF (NOT "X${TARGET_SHLIB_MAJOR}${TARGET_SHLIB_MINOR}X" MATCHES "XX")

        ELSE (__sources_1)
            DEBUGMESSAGE(1 "ADD_EXECUTABLE_EX(${exename}) @ ${CMAKE_CURRENT_SOURCE_DIR}: no sources found. No executables will be added")
        ENDIF (__sources_1)

        DEBUGMESSAGE(1 "ADD_EXECUTABLE_EX(${exename}) @ ${CMAKE_CURRENT_SOURCE_DIR} succeeded")
    ELSE (NOT __nobuild_)
        DEBUGMESSAGE(1 "ADD_EXECUTABLE_EX(${exename}) @ ${CMAKE_CURRENT_SOURCE_DIR} skipped (see '${ARCADIA_BUILD_ROOT}/excl_target.list' for details)")
    ENDIF (NOT __nobuild_)

    ON_TARGET_FINISH()
ENDMACRO (ADD_EXECUTABLE_EX)

# =============================================================================
# ADD_LIBRARY_EX(libname)
#
MACRO (ADD_LIBRARY_EX libname)
    CHECK_PROJECT_DEFINED(${libname} LIBRARY)
    CHECK_SRCS(${ARGN})
    IF (NOT __nobuild_)
        SET(PROJECTNAME ${libname})
#        INCLUDE(${ARCADIA_ROOT}/cmake/include/dtmk.cmake)
        EXEC_DTMK()

        STRIP_KEYWORDS(__sources_1 __keywords_ ${ARGN})
#       SET(__sources_1 ${ARGN})
        SEPARATE_ARGUMENTS(__sources_1)
        IF (NOT __sources_1 AND PEERDIR)
            # Adding
            SET_APPEND(__sources_1 "${ARCADIA_ROOT}/cmake/include/_cmake_fake_src.cpp")
            DEBUGMESSAGE(2 "Adding fictive source file to library ${PROJECTNAME} to inherit PEERDIRs")
        ENDIF (NOT __sources_1 AND PEERDIR)
        IF (__sources_1)

            PREPARE_SRC_FILES(__sources_1 SRCDIR ${ARCADIA_ROOT})

            _APPLY_DEFS(${__sources_1})

            IF (NOT "X${TARGET_SHLIB_MAJOR}${TARGET_SHLIB_MINOR}X" MATCHES "XX" AND MAKE_ONLY_SHARED_LIB)
                SET(DTMK_LIBTYPE SHARED)
            ENDIF (NOT "X${TARGET_SHLIB_MAJOR}${TARGET_SHLIB_MINOR}X" MATCHES "XX" AND MAKE_ONLY_SHARED_LIB)

            IF ("${DTMK_LIBTYPE}" MATCHES "SHARED")
                lorder_for_peerlibs(${libname} PEERLIBS PEERDEPENDS __sources_1)
            ELSE ("${DTMK_LIBTYPE}" MATCHES "SHARED")
                # No need for PEERDEPENDS in STATIC libraries
                #SET(PEERDEPENDS)
            ENDIF ("${DTMK_LIBTYPE}" MATCHES "SHARED")

            CHECK_DUPLICATE_TARGET(${libname})
            ADD_LIBRARY(${libname} ${DTMK_LIBTYPE} ${__keywords_} ${__sources_1})
            SET_IDE_FOLDER(${libname})

            IF ("${DTMK_LIBTYPE}" MATCHES "SHARED")
                IF (LOCAL_SHARED_LIBRARY_SUFFIX)
                    SET_TARGET_PROPERTIES(${libname} PROPERTIES
                        SUFFIX "${LOCAL_SHARED_LIBRARY_SUFFIX}"
                    )
                    DEBUGMESSAGE(2 "Changed SUFFIX[${LOCAL_SHARED_LIBRARY_SUFFIX}] for ${libname}")
                ENDIF (LOCAL_SHARED_LIBRARY_SUFFIX)
                IF (LOCAL_SHARED_LIBRARY_PREFIX)
                    SET_TARGET_PROPERTIES(${libname} PROPERTIES
                        PREFIX "${LOCAL_SHARED_LIBRARY_PREFIX}"
                    )
                    DEBUGMESSAGE(2 "Changed PREFIX[${LOCAL_SHARED_LIBRARY_PREFIX}] for ${libname}")
                ENDIF (LOCAL_SHARED_LIBRARY_PREFIX)
            ENDIF ("${DTMK_LIBTYPE}" MATCHES "SHARED")

            GET_TARGET_PROPERTY(__realname_ ${libname} LOCATION)
            DEBUGMESSAGE(3 "Realname for ${libname} is [${__realname_}]")

#           SET_TARGET_PROPERTIES(${libname} PROPERTIES
#                   COMPILE_FLAGS "${__LOCAL_COMPILE_FLAGS}")

            IF (NOT "X${TARGET_SHLIB_MAJOR}${TARGET_SHLIB_MINOR}X" MATCHES "XX")
                SET_TARGET_PROPERTIES(${libname} PROPERTIES
                    VERSION "${TARGET_SHLIB_MAJOR}" #.${TARGET_SHLIB_MINOR}"
                )
            ENDIF (NOT "X${TARGET_SHLIB_MAJOR}${TARGET_SHLIB_MINOR}X" MATCHES "XX")

#           IF (INSTALLEDNAME)
#               SET_TARGET_PROPERTIES(${libname} PROPERTIES
#                   OUTPUT_NAME ${INSTALLEDNAME})
#           ENDIF (INSTALLEDNAME)

            APPLY_PEERDIR_SUBDIR(${libname})
            GETTARGETNAME(__this_ ${libname})

            IF (LOCAL_WEAKNAME_LIB)
                GET_GLOBAL_DIRNAME(__libname_ ${libname})
                DEBUGMESSAGE(2 "Setting ${__libname_}_WEAKNAME_LIB to ${LOCAL_WEAKNAME_LIB}")
                SET(${__libname_}_WEAKNAME_LIB ${LOCAL_WEAKNAME_LIB} CACHE INTERNAL "" FORCE)
            ENDIF (LOCAL_WEAKNAME_LIB)

#            IF (NOT ${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB)
                SET(${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB ${libname} CACHE INTERNAL "" FORCE)
                DEBUGMESSAGE(2 "${libname}: PEERDIR in CURDIR will set, ${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB is [${libname}]")
#            ELSE (NOT ${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB)
#                DEBUGMESSAGE(2 "${libname}: PEERDIR in CURDIR already set, ${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB is [${${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB}]")
#            ENDIF (NOT ${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB)

            DEBUGMESSAGE(3 "library[${DTMK_LIBTYPE}], INSTALLDIR[${INSTALLDIR}], INSTALLEDNAME[${INSTALLEDNAME}]")
            IF ("${DTMK_LIBTYPE}" MATCHES "SHARED")
                SET(VERSION_SUFFIX)
                IF (NOT "X${TARGET_SHLIB_MAJOR}X" STREQUAL "XX")
                    SET(VERSION_SUFFIX .${TARGET_SHLIB_MAJOR})
                ENDIF (NOT "X${TARGET_SHLIB_MAJOR}X" STREQUAL "XX")

                IF ("${CURDIR}" STREQUAL "${BINDIR}")
                     # Do not make symlink to library
                     ENABLE(__nolink_local_)
                ENDIF ("${CURDIR}" STREQUAL "${BINDIR}")
                IF (NOT NOLINK AND NOT __nolink_local_ AND PLATFORM_SUPPORTS_SYMLINKS)
                    GET_TARGET_PROPERTY(__realname_ ${libname} LOCATION)
                    GET_FILENAME_COMPONENT(__realname_ ${__realname_} NAME)
                    ADD_CUSTOM_COMMAND(TARGET ${libname}
                        POST_BUILD
                        COMMAND ln -sf ${__this_} ${CMAKE_CURRENT_SOURCE_DIR}/${__realname_}
                        COMMENT "Making a symbolic link from ${__this_} to ${CMAKE_CURRENT_SOURCE_DIR}/${__realname_}"
                        )
                    SET_APPEND(DTMK_CLEANFILES ${CMAKE_CURRENT_SOURCE_DIR}/${__realname_})
                ENDIF (NOT NOLINK AND NOT __nolink_local_ AND PLATFORM_SUPPORTS_SYMLINKS)

                IF (INSTALLDIR)
                    SET(__targetname_ ${GLOBAL_TARGET_NAME})

                    IF (INSTALLEDNAME)
                        IF (PLATFORM_SUPPORTS_SYMLINKS)
                            SET(FULLINSTALLEDNAME ${CMAKE_SHARED_LIBRARY_PREFIX}${INSTALLEDNAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
                            DEBUGMESSAGE(3 "library[${libname}]: INSTALL(PROGRAMS ${__this_}${VERSION_SUFFIX} DESTINATION ${INSTALLDIR} RENAME ${FULLINSTALLEDNAME}${VERSION_SUFFIX}")
                            INSTALL(TARGETS ${libname}
                                LIBRARY DESTINATION ${INSTALLDIR}
                            )
                            IF (NOT WIN32 AND "${CMAKE_BUILD_TYPE}" MATCHES "Release")
                                IF (NOT NOSTRIP)
                                    INSTALL(CODE "EXECUTE_PROCESS(COMMAND strip ${FULLINSTALLEDNAME}${VERSION_SUFFIX}  WORKING_DIRECTORY ${INSTALLDIR})")
                                ENDIF (NOT NOSTRIP)
                                IF (BUILD_YSERVER_PACKAGE)
                                    INSTALL(CODE "EXECUTE_PROCESS(COMMAND chcon -t  textrel_shlib_t ${FULLINSTALLEDNAME}${VERSION_SUFFIX}  WORKING_DIRECTORY ${INSTALLDIR})")
                                ENDIF (BUILD_YSERVER_PACKAGE)
                            ENDIF (NOT WIN32 AND "${CMAKE_BUILD_TYPE}" MATCHES "Release")
                        ELSE(PLATFORM_SUPPORTS_SYMLINKS)
                            SET(FULLINSTALLEDNAME ${CMAKE_SHARED_LIBRARY_PREFIX}${INSTALLEDNAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
                            DEBUGMESSAGE(3 "library[${libname}]: INSTALL(PROGRAMS ${__this_}${VERSION_SUFFIX} DESTINATION ${INSTALLDIR} RENAME ${FULLINSTALLEDNAME}")
                            INSTALL(PROGRAMS ${ARCADIA_BUILD_ROOT}/bin/${CMAKE_BUILD_TYPE}/${libname}.dll
                                DESTINATION ${INSTALLDIR}
                                RENAME ${FULLINSTALLEDNAME}
                            )
                        ENDIF(PLATFORM_SUPPORTS_SYMLINKS)
                    ENDIF (INSTALLEDNAME)
                ENDIF (INSTALLDIR)

                IF (PERLXSNAME AND INSTALLDIR)
                    DEBUGMESSAGE(3 "perlxs, ${__this_} \"${PERLINSTALLSITEARCH}\" \"${SHLIBNAME}\" ${libname}")

                    STRING(REGEX REPLACE "::" "/" PERLXSNAME ${PERLXSNAME})
                    STRING(REGEX REPLACE "(.*)/.*" "\\1" PERLXSDIR ${PERLXSNAME})
                    STRING(REGEX REPLACE ".*/(.*)" "\\1" PERLXSNAME ${PERLXSNAME})
                    IF (PERLXSDIR STREQUAL PERLXSNAME)
                        SET(PERLXSDIR "")
                    ENDIF (PERLXSDIR STREQUAL PERLXSNAME)

                    INSTALL(FILES ${CMAKE_CURRENT_SOURCE_DIR}/${PERLXSNAME}.pm
                        DESTINATION ${INSTALLDIR}/${PERLINSTALLSITEARCH}/${PERLXSDIR}/)
                    INSTALL(FILES ${__this_}${VERSION_SUFFIX}
                        DESTINATION ${INSTALLDIR}/${PERLINSTALLSITEARCH}/auto/${PERLXSDIR}/${PERLXSNAME}/
                        RENAME ${PERLXSNAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
                ENDIF (PERLXSNAME AND INSTALLDIR)

                IF (PLATFORM_SUPPORTS_SYMLINKS)
                    SET(SHLIBNAME_LINK)
                    # Make symlinks:
                    #   LIBSYMLINK_DIR/libname.major -> ${__this_}
                    #   LIBSYMLINK_DIR/libname -> libname.major
                    # Vars:
                    #   SHLIBNAME - lib name with major version number
                    #   SHLIBNAME_LINK - lib name without version numbers (just an .so-extension)
                    IF (NOT "${CMAKE_BUILD_TYPE}" MATCHES "Profile")

                        GET_FILENAME_COMPONENT(SHLIBNAME_LINK ${__this_} NAME)
                        DEBUGMESSAGE(3 "SHLIBNAME_LINK=[${SHLIBNAME_LINK}]")


                        ADD_CUSTOM_COMMAND(TARGET ${libname} POST_BUILD
                            COMMAND mkdir -p "${LIBSYMLINK_DIR}"
                            COMMENT "mkdir -p '${LIBSYMLINK_DIR}'"
                            VERBATIM
                        )

                        IF (DEFINED TARGET_SHLIB_MAJOR)
                            GET_FILENAME_COMPONENT(SHLIBNAME      ${__this_} NAME_WE)
                            SET(SHLIBNAME ${SHLIBNAME}${CMAKE_SHARED_LIBRARY_SUFFIX}.${TARGET_SHLIB_MAJOR})
                            DEBUGMESSAGE(3 "SHLIBNAME=[${SHLIBNAME}]")
                            ADD_CUSTOM_COMMAND(TARGET ${libname} POST_BUILD
                                COMMAND ln -sf ${__this_}${VERSION_SUFFIX} ${SHLIBNAME}
                                WORKING_DIRECTORY ${LIBSYMLINK_DIR}
                                COMMENT "Making symlinks in ${LIBSYMLINK_DIR} for ${SHLIBNAME_LINK}"
                                VERBATIM
                            )
                            ADD_CUSTOM_COMMAND(TARGET ${libname} POST_BUILD
                                COMMAND ln -sf ${SHLIBNAME} ${SHLIBNAME_LINK}
                                WORKING_DIRECTORY ${LIBSYMLINK_DIR}
                                COMMENT "Making symlinks in ${LIBSYMLINK_DIR} for ${SHLIBNAME}"
                                VERBATIM
                            )
                        ELSE (DEFINED TARGET_SHLIB_MAJOR)
                            ADD_CUSTOM_COMMAND(TARGET ${libname} POST_BUILD
                                COMMAND ln -sf ${__this_} ${SHLIBNAME_LINK}
                                WORKING_DIRECTORY ${LIBSYMLINK_DIR}
                                COMMENT "Making symlinks in ${LIBSYMLINK_DIR} for ${SHLIBNAME_LINK}"
                                VERBATIM
                            )
                        ENDIF (DEFINED TARGET_SHLIB_MAJOR)


                        SET_APPEND(DTMK_CLEANFILES ${LIBSYMLINK_DIR}/${SHLIBNAME_LINK} ${LIBSYMLINK_DIR}/${SHLIBNAME})
                    ENDIF (NOT "${CMAKE_BUILD_TYPE}" MATCHES "Profile")
                ENDIF (PLATFORM_SUPPORTS_SYMLINKS)

            ENDIF ("${DTMK_LIBTYPE}" MATCHES "SHARED")

            DEBUGMESSAGE(1 "ADD_LIBRARY_EX(${libname} ${DTMK_LIBTYPE}) @ ${CMAKE_CURRENT_SOURCE_DIR} succeeded")
        ELSE (__sources_1)
            DEBUGMESSAGE(1 "ADD_LIBRARY_EX(${libname} ${DTMK_LIBTYPE}) @ ${CMAKE_CURRENT_SOURCE_DIR}: no sources found. No libraries will be added")
        ENDIF (__sources_1)
    ELSE (NOT __nobuild_)
        DEBUGMESSAGE(1 "ADD_LIBRARY_EX(${libname} ${DTMK_LIBTYPE}) @ ${CMAKE_CURRENT_SOURCE_DIR} skipped (see '${ARCADIA_BUILD_ROOT}/excl_target.list' for details)")
    ENDIF (NOT __nobuild_)

    ON_TARGET_FINISH()
ENDMACRO (ADD_LIBRARY_EX)

# =============================================================================
# ADD_LIBRARY_EX1(prjname)
#   difference from ADD_LIBRARY_EX: appends parent directory name to the prjname
#
MACRO (ADD_LIBRARY_EX1 libname)
    ADD_LIBRARY_EX(${REALPRJNAME} ${ARGN})
ENDMACRO (ADD_LIBRARY_EX1 libname)

# =============================================================================
# APPLY_PEERDIR_SUBDIR(prjname)
#
MACRO(APPLY_PEERDIR_SUBDIR prjname)
    FOREACH(__itemprop_ ${CHECK_TARGETPROPERTIES})
        SET_TARGET_PROPERTIES(${prjname} PROPERTIES
            ${__itemprop_} "${${__itemprop_}}")
    ENDFOREACH(__itemprop_)

    # Подвязываем список библиотек
    DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: PEERDIR=[${PEERDIR}]")
    DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: PEERLIBS=[${PEERLIBS}]")
    SET(__peernames_)
    SET(__prop_error_overall_)
    FOREACH(__item_ ${PEERDEPENDS})
        GET_TARGET_PROPERTY(__prop_ ${__item_} TYPE)
        SET_APPEND(__peernames_ "${__item_}:${__prop_}")

        FOREACH(__itemprop_ ${CHECK_TARGETPROPERTIES})
            GET_TARGET_PROPERTY(__prop_ ${__item_} ${__itemprop_})
            SET(__prop_error_ no)
            IF (__prop_ AND NOT ${__itemprop_})
                SET(__prop_error_ yes)
            ELSEIF (NOT __prop_ AND ${__itemprop_})
                SET(__prop_error_ yes)
            ENDIF (__prop_ AND NOT ${__itemprop_})
            IF (__prop_error_)
                SET_APPEND(__prop_error_overall_ COMMAND echo "Targets ${prjname} and ${__item_} have different ${__itemprop_} property (${${__itemprop_}}/${__prop_})")
                MESSAGE(STATUS "Error: Targets ${prjname} and ${__item_} have different ${__itemprop_} property (${${__itemprop_}}/${__prop_})")
            ENDIF (__prop_error_)
        ENDFOREACH(__itemprop_)
    ENDFOREACH(__item_ ${PEERDEPENDS})
    IF (__prop_error_overall_)
        ADD_CUSTOM_COMMAND(TARGET ${prjname}
            PRE_BUILD
            ${__prop_error_overall_}
            COMMAND false
            VERBATIM
        )
    ENDIF (__prop_error_overall_)
    DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: PEERDEPENDS=[${__peernames_}]")
    DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: OBJADDE=[${OBJADDE}]")

    IF (THISPROJECTDEPENDS)
        DEBUGMESSAGE(2 "${prjname} depends on [${THISPROJECTDEPENDS}] (set in BUILDAFTER)")
        ADD_DEPENDENCIES(${prjname} ${THISPROJECTDEPENDS})
    ENDIF (THISPROJECTDEPENDS)

    IF (NOT USE_WEAK_DEPENDS)
        #GET_TARGET_PROPERTY(__prop_ ${prjname} TYPE)

        TARGET_LINK_LIBRARIES(${prjname} ${OBJADDE})

        IF (PEERDEPENDS)# AND NOT "${__prop_}" STREQUAL "STATIC_LIBRARY")
            IF (NOT WIN32 OR NOT ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 2.8)
                TARGET_LINK_LIBRARIES(${prjname} ${PEERDEPENDS})
            ENDIF (NOT WIN32 OR NOT ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 2.8)

            ADD_DEPENDENCIES(${prjname} ${PEERDEPENDS})
        ENDIF (PEERDEPENDS)# AND NOT "${__prop_}" STREQUAL "STATIC_LIBRARY")

    ELSE (NOT USE_WEAK_DEPENDS)

        SET(PEERDEPENDS_NEW)
        SET(OBJADDE_NEW)
        FOREACH (__peer_ ${PEERDEPENDS})
            GET_TARGET_PROPERTY(__peer_value_ ${__peer_} PEERDEPENDS)
            IF (NOT __peer_value_)
                SET( __peer_value_)
            ENDIF (NOT  __peer_value_)
            GET_TARGET_PROPERTY(__obj_value_ ${__peer_} OBJADDE)
            IF (NOT __obj_value_)
                SET( __obj_value_)
            ENDIF (NOT  __obj_value_)
            SET(PEERDEPENDS_NEW ${PEERDEPENDS_NEW} ${__peer_value_})
            SET(OBJADDE_NEW ${OBJADDE_NEW} ${__obj_value_})
        ENDFOREACH (__peer_ ${PEERDEPENDS})

        DEBUGMESSAGE(2 "${prjname}'s inherited PEERDEPENDS[${PEERDEPENDS_NEW}]")
        DEBUGMESSAGE(2 "${prjname}'s PEERDEPENDS[${PEERDEPENDS}]")
        DEBUGMESSAGE(2 "${prjname}'s inherited OBJADDE[${OBJADDE_NEW}]")
        DEBUGMESSAGE(2 "${prjname}'s OBJADDE[${OBJADDE}]")

        SET(__objadde_ ${OBJADDE_NEW} ${OBJADDE})
        LIST_REMOVE_DUPLICATES(__objadde_ "^-\([lL].*\)|^-\(Wl,[-]E\)|\(kthread\)|\(rdynamic\)")

        SET(__peers_ ${PEERDEPENDS_NEW} ${PEERDEPENDS})
        LIST_REMOVE_DUPLICATES(__peers_)

        SET(__peers2_)
        FOREACH (__p1_ ${__peers_})
            GET_GLOBAL_DIRNAME(__p_ ${__p1_})
            IF ("X${${__p_}_WEAKNAME_LIB}X" STREQUAL "XX")
                SET_APPEND(__peers2_ ${__p1_})
            ELSE ("X${${__p_}_WEAKNAME_LIB}X" STREQUAL "XX")
                GET_GLOBAL_DIRNAME(__dir_ ${ARCADIA_ROOT}/${${__p_}_WEAKNAME_LIB})
                SET(__w_ ${${__dir_}_DEPENDNAME_LIB})
                LIST(FIND __peers_ "${__w_}" __pos_)
                IF (__pos_ EQUAL -1)
                    SET_APPEND(__peers2_ ${__p1_})
                ELSE (__pos_ EQUAL -1)
                    DEBUGMESSAGE(2 "${CURDIR}: PEERDIR ${__p1_} is weak version of ${__w_}. Will leave only the last")
                ENDIF (__pos_ EQUAL -1)
            ENDIF ("X${${__p_}_WEAKNAME_LIB}X" STREQUAL "XX")
        ENDFOREACH (__p1_)
        SET(__peers_ ${__peers2_})

        GET_TARGET_PROPERTY(__prop_ ${prjname} TYPE)

        IF (NOT "${__prop_}" STREQUAL "STATIC_LIBRARY")
            TARGET_LINK_LIBRARIES(${prjname} ${__objadde_})
            IF (NOT "X${__peers_}X" STREQUAL "XX")
                ADD_DEPENDENCIES(${prjname} ${__peers_})
                SET(${prjname}_PEERLIBS ${__peers_})
                IF (NOT WIN32 OR NOT ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 2.8)
                    TARGET_LINK_LIBRARIES(${prjname} ${__peers_})
                ENDIF (NOT WIN32 OR NOT ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} LESS 2.8)
            ENDIF (NOT "X${__peers_}X" STREQUAL "XX")
            DEBUGMESSAGE(2 "${prjname} use PEERDEPENDS[${__peers_}]")
            DEBUGMESSAGE(2 "${prjname} use OBJADDE[${__objadde_}]")
        ELSE (NOT "${__prop_}" STREQUAL "STATIC_LIBRARY")
            IF (__objadde_)
                SET_TARGET_PROPERTIES(${prjname} PROPERTIES
                    OBJADDE "${__objadde_}"
                )
            ENDIF (__objadde_)
            IF (__peers_)
                SET(${prjname}_PEERLIBS ${__peers_})
                SET_TARGET_PROPERTIES(${prjname} PROPERTIES
                    PEERDEPENDS "${__peers_}"
                )
            ENDIF (__peers_)

            DEBUGMESSAGE(2 "${prjname} is a static library. Will save PEERDEPENDS[${PEERDEPENDS}]")
            DEBUGMESSAGE(2 "${prjname} is a static library. Will save OBJADDE[${OBJADDE}]")
        ENDIF (NOT "${__prop_}" STREQUAL "STATIC_LIBRARY")

    ENDIF (NOT USE_WEAK_DEPENDS)

    SET(__dtmk_l_ ${DTMK_L})
    SEPARATE_ARGUMENTS_SPACE(__dtmk_l_)
    DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: DTMK_L=[${DTMK_L}]")
    SET_TARGET_PROPERTIES(${prjname} PROPERTIES
        LINK_FLAGS "${__dtmk_l_}")

    IF (USE_GOOGLE_ALLOCATOR)
        SET_TARGET_PROPERTIES(${prjname}
            PROPERTIES LINKER_LANGUAGE CXX)
    ENDIF (USE_GOOGLE_ALLOCATOR)

    IF (USE_J_ALLOCATOR)
        SET_TARGET_PROPERTIES(${prjname}
            PROPERTIES LINKER_LANGUAGE CXX)
    ENDIF (USE_J_ALLOCATOR)

    GET_TARGET_PROPERTY(__linker_lang_ ${prjname} LINKER_LANGUAGE)
    IF (NOT __linker_lang_)
        SET_TARGET_PROPERTIES(${prjname}
            PROPERTIES LINKER_LANGUAGE CXX)
        DEBUGMESSAGE(3 "${CMAKE_CURRENT_SOURCE_DIR}: LINKER_LANGUAGE for ${prjname} set to CXX")
    ENDIF (NOT __linker_lang_)

    IF (${prjname}_OUTPUTNAME)
        SET_TARGET_PROPERTIES(
            ${prjname} PROPERTIES
                OUTPUT_NAME ${${prjname}_OUTPUTNAME}
        )
        DEBUGMESSAGE(3 "${CURDIR}: OUTPUT_NAME=[${${prjname}_OUTPUTNAME}]")
    ENDIF (${prjname}_OUTPUTNAME)

    SET(__targetname_ ${GLOBAL_TARGET_NAME})
    GETTARGETNAME(__location_ ${prjname})
    FILE(APPEND ${TARGET_LIST_FILENAME} "${prjname} ${CURDIR} ${__location_} ${BINDIR}\n")

    # Unittests per directory
    IF (UT_PERDIR)
    SET(UT_SRCS)
    FILE(GLOB UT_SRCS "${CURDIR}/*_ut.cpp" "${CURDIR}/*_ut.c" "${CURDIR}/*_ut.proto")

    # Ignore problem files like .file_ut.cpp ~file_ut.cpp
    FOREACH(__ut__ ${UT_SRCS})
        STRING(REGEX MATCH "^.*/((\\.[^/]+)|([^/]*~[^/]+))$" __thrash__ ${__ut__})
        IF(__thrash__)
            LIST(REMOVE_ITEM UT_SRCS ${__thrash__})
        ENDIF(__thrash__)
    ENDFOREACH(__ut__ ${UT_SRCS})

    SET(UTMAIN_NAME "${ARCADIA_ROOT}/library/unittest/utmain.cpp")
    IF (UT_SRCS AND EXISTS "${UTMAIN_NAME}")
        SET(UT_DIR ${BINDIR}/${prjname}${UT_SUFFIX}.dir)
        FILE(MAKE_DIRECTORY ${UT_DIR})
        SET(UT_PRJNAME ${prjname})
        IF ("${SAVEDTYPE}" STREQUAL "LIB")
            FILE(RELATIVE_PATH UT_PERDIR_PEERDIR ${ARCADIA_ROOT} ${CURDIR})
            SET(UT_PERDIR_PEERDIR "PEERDIR(${UT_PERDIR_PEERDIR})")
            DEBUGMESSAGE(3 "${CURDIR}: UT_PERDIR_PEERDIR[${UT_PERDIR_PEERDIR}]")
        ENDIF("${SAVEDTYPE}" STREQUAL "LIB")
        CONFIGURE_FILE(
            ${ARCADIA_ROOT}/cmake/include/ut_template.cmake
            ${UT_DIR}/CMakeLists.txt
            @ONLY
        )
        FILE(APPEND ${UNITTEST_LIST_FILENAME} "${UT_DIR};${UT_DIR};")
    ELSEIF (NOT EXISTS "${UTMAIN_NAME}")
        IF (NOT ROBOT_BUILD)
            MESSAGE(STATUS "\"${UTMAIN_NAME}\" not exists. Per-dir unittests will be disabled")
        ENDIF (NOT ROBOT_BUILD)
    ENDIF (UT_SRCS AND EXISTS "${UTMAIN_NAME}")
    ENDIF (UT_PERDIR)

    IF (UT_PEERDIR)
        MESSAGE(FATAL_ERROR "Variable UT_PEERDIR is wrong. Probably you meant UT_PERDIR")
    ENDIF (UT_PEERDIR)
ENDMACRO(APPLY_PEERDIR_SUBDIR)

# =============================================================================
# ADD_EXECUTABLE_FICT(exename sources)
# This macro adds the fictive executable.
# Useful in non-exe/library projects (for ex., dict/zaliznak)
#
MACRO (ADD_EXECUTABLE_FICT exename)
    STRIP_KEYWORDS(__targets_ __keywords_ ${ARGN})

    ADD_CUSTOM_COMMAND(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${exename}_fict_source.cpp
        COMMAND cp ${ARCADIA_ROOT}/cmake/include/fict_exe_source.cpp ${CMAKE_CURRENT_BINARY_DIR}/${exename}_fict_source.cpp
        DEPENDS ${__targets_}
        COMMENT "All DEPENDS for ${exename} are ready. Preparing fictive source..."
        )

    #SET_SOURCE_FILES_PROPERTIES(${CMAKE_CURRENT_BINARY_DIR}/${exename}_fict_source.cpp
    #   PROPERTIES GENERATED yes)

    SET(SRCS ${CMAKE_CURRENT_BINARY_DIR}/${exename}_fict_source.cpp)

    ADD_EXECUTABLE_EX(${exename} ${__keywords_} ${SRCS})
ENDMACRO (ADD_EXECUTABLE_FICT)


# =============================================================================
# CREATE_SHARED_LIBRARY(relative_path)
MACRO (CREATE_SHARED_LIBRARY)
    IF (GLOBAL_TARGET_NAME)
        IF (NOT CURTARGET_EXCLUDED)
            IF (${ARGN})
                SET(__folder_name_ ${ARGV1})
            ELSE (${ARGN})
                SET(__folder_name_ "shared")
            ENDIF (${ARGN})
            SET(__global_target_name_shared_ "${GLOBAL_TARGET_NAME}_${__folder_name_}")

            GET_TARGET_PROPERTY(__target_type_ ${LASTPRJNAME} TYPE)
            IF ("${__target_type_}" STREQUAL "STATIC_LIBRARY")
                # Create shared library
                SET(SHLIB_BIN_DIR "${CMAKE_CURRENT_BINARY_DIR}/${__folder_name_}")
                SET(DUMMY_SHARED_PRJNAME ${LASTPRJNAME}_${__folder_name_})
                SET(GLOBAL_TARGET_NAME "${__global_target_name_shared_}")
                SET(LASTPRJNAME ${DUMMY_SHARED_PRJNAME})
                SET_APPEND(PROJECT_DEFINED ${DUMMY_SHARED_PRJNAME})
                ENABLE(MAKE_ONLY_SHARED_LIB)
                ADD_LIBRARY_EX(${DUMMY_SHARED_PRJNAME} ${SRCS})
            ELSEIF ("${__target_type_}" STREQUAL "SHARED_LIBRARY")
                SET(${__global_target_name_shared_}_DEPENDNAME_LIB ${${GLOBAL_TARGET_NAME}_DEPENDNAME_LIB} CACHE INTERNAL "" FORCE)

                DEBUGMESSAGE(2 "${LASTPRJNAME} (refer to GETTARGETNAME(__varname_ ${LASTPRJNAME}))")
            ELSE ("${__target_type_}" STREQUAL "STATIC_LIBRARY")
                MESSAGE(SEND_ERROR "Target defined is not a LIBRARY")
            ENDIF ("${__target_type_}" STREQUAL "STATIC_LIBRARY")
        ELSE (NOT CURTARGET_EXCLUDED)
            DEBUGMESSAGE(2 "${GLOBAL_TARGET_NAME} is excluded from build. CREATE_SHARED_LIBRARY will not add an extra target")
        ENDIF (NOT CURTARGET_EXCLUDED)
    ELSE (GLOBAL_TARGET_NAME)
        MESSAGE(SEND_ERROR "No target defined at the moment")
    ENDIF (GLOBAL_TARGET_NAME)
ENDMACRO (CREATE_SHARED_LIBRARY)

# =============================================================================
# INIT_CONFIG path_to_root
# This macro should be just before the PROJECT_EX directive
#
MACRO (INIT_CONFIG)
    FILE(RELATIVE_PATH __var_ "${CMAKE_CURRENT_SOURCE_DIR}" "${ARCADIA_ROOT}/CMakeLists.txt")
    GET_FILENAME_COMPONENT(__var_ ${__var_} PATH)
    DEBUGMESSAGE(4 "PathToRoot[${__var_}] for ${CMAKE_CURRENT_SOURCE_DIR}")

    SET(PATH_TO_ROOT ${__var_})

    INCLUDE_FROM(local.cmake ${ARCADIA_ROOT}/.. ${ARCADIA_ROOT} ${ARCADIA_BUILD_ROOT}/.. ${ARCADIA_BUILD_ROOT})
ENDMACRO (INIT_CONFIG)

# =============================================================================
# ADD_YTEST
# This macro check targetname before ADD_TEST and implement some common tasks
#
MACRO (ADD_YTEST test_name script_rel_path binary_path)
    IF ("${ARGN}" STREQUAL "ignored")
            MESSAGE(STATUS "INFO: test " ${test_name} " is IGNORED.")
    ELSE ("${ARGN}" STREQUAL "ignored")
        DEBUGMESSAGE(1, "Test ${test_name} ${script_rel_path} ${binary_path}")
        IF (LASTPRJNAME AND NOT SAVEDNAME)
            SET(YTEST_CUSTOM_DEPENDS "null")
            IF (${test_name}_DEPENDS)
                DEBUGMESSAGE(1, "Test ${test_name} has dependency list: ${${test_name}_DEPENDS}")
                FOREACH(__item_ ${${test_name}_DEPENDS})
                    SET(YTEST_CUSTOM_DEPENDS "${YTEST_CUSTOM_DEPENDS}\;${__item_}")
                ENDFOREACH(__item_)
            ENDIF (${test_name}_DEPENDS)

            GETTARGETNAME(__this_)
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "TEST-NAME: ${test_name}\n")
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "SCRIPT-REL-PATH: ${script_rel_path}\n")
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "TESTED-PROJECT-NAME: ${LASTPRJNAME}\n")
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "SOURCE-FOLDER-PATH: ${CMAKE_CURRENT_SOURCE_DIR}\n")
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "BUILD-FOLDER-PATH: ${CMAKE_CURRENT_BINARY_DIR}\n")
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "BINARY-PATH: ${binary_path}\n")
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "CUSTOM-DEPENDENCIES: ${YTEST_CUSTOM_DEPENDS}\n")
            FILE (APPEND ${TEST_DART_TMP_FILENAME} "=============================================================\n")

            FILE(APPEND ${TEST_LIST_FILENAME} "${LASTPRJNAME} ${CURDIR} ${binary_path} ${test_name}\n")
        ELSE (LASTPRJNAME AND NOT SAVEDNAME)
            MESSAGE(SEND_ERROR "Unknown targetname, move MACRO ADD_TEST_EX AFTER END(): " ${CURDIR} )
        ENDIF (LASTPRJNAME AND NOT SAVEDNAME)
    ENDIF ("${ARGN}" STREQUAL "ignored")
ENDMACRO (ADD_YTEST)

# =============================================================================
# OPTION_NEEDS
# If option1 is set and option2 is not, then send an error.
#
MACRO(OPTION_NEEDS option1 option2)
    IF (${option1} AND NOT ${option2})
        MESSAGE(SEND_ERROR "Option ${option1} needs ${option2}")
    ENDIF (${option1} AND NOT ${option2})
ENDMACRO(OPTION_NEEDS)

# =============================================================================
# OPTION_TO_CFLAGS
# Add option to CFLAGS.
#
MACRO(OPTION_TO_CFLAGS option)
    IF (${option})
        IF (NOT "${ARGV1}" STREQUAL "")
            CFLAGS(-D${ARGV1})
        ELSE (NOT "${ARGV1}" STREQUAL "")
            CFLAGS(-D${option})
        ENDIF (NOT "${ARGV1}" STREQUAL "")
    ENDIF (${option})
ENDMACRO(OPTION_TO_CFLAGS)

MACRO(USEIDL path)
    PEERDIR(ADDINCL contrib/libs/protobuf)
    PEERDIR(ADDINCL library/eventlog)
    BUILDAFTER(library/eventlog)
    #please use a root-based path for generated files
    #PEERDIR(ADDINCL ${path})
    PEERDIR(${path})
    BUILDAFTER(${path})
ENDMACRO(USEIDL)

ENABLE(USE_OPTIMIZATION)
MACRO(OPTIMIZE)
    IF (USE_OPTIMIZATION AND NOT WIN32)
        CFLAGS(${ARGN})
    ENDIF (USE_OPTIMIZATION AND NOT WIN32)
ENDMACRO(OPTIMIZE)

MACRO (GENERATE_ENUM_SERIALIZATION)
    SET(__first yes)
    FOREACH(__item ${ARGN})
        IF (__first)
            SET(file_for_parsing "${__item}")
            SET(__first no)
        ELSE (__first)
            SET(ns "${__item}")
        ENDIF (__first)
    ENDFOREACH(__item)

    SET(LUA_SCRIPT ${ARCADIA_ROOT}/cmake/include/parseenum.lua)
    GET_FILENAME_COMPONENT(name ${file_for_parsing} NAME_WE)
    SET(FILE_GENERATED ${CMAKE_CURRENT_BINARY_DIR}/${name}_serialized.cpp)

    TOOLDIR_EX(
        tools/lua lua
    )

    ADD_CUSTOM_COMMAND(
        OUTPUT ${FILE_GENERATED}
        COMMAND ${lua} ${LUA_SCRIPT}
            ${CMAKE_CURRENT_SOURCE_DIR}/${file_for_parsing}
            ${FILE_GENERATED}
            ${ns}
            || ${RM} ${FILE_GENERATED}
        MAIN_DEPENDENCY "${file_for_parsing}"
        DEPENDS lua ${file_for_parsing} ${LUA_SCRIPT}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Building serialization of \"${file_for_parsing}\" with lua..."
    )

    SET_APPEND(SRCS "${FILE_GENERATED}")
    SOURCE_GROUP("Generated" FILES ${FILE_GENERATED})

ENDMACRO (GENERATE_ENUM_SERIALIZATION)

# =============================================================================
# MAKE_PACKAGE
# Creates debian package (to be extended).
#
MACRO(MAKE_PACKAGE prjdir)
    ENTER_PROJECT()
    RECURSE(${prjdir})
    STRING(REGEX REPLACE ".*[/]" "" PROJECTNAME ${prjdir})

    PROJECT_EX2(${PROJECTNAME}_package)

    IF (WIN32)
        MESSAGE("Windows package construction is under construction")
    ELSE (WIN32)
        EXECUTE_PROCESS(OUTPUT_VARIABLE __tmp_
            COMMAND which fakeroot
            RESULT_VARIABLE __fakeroot_check_result_
        )
        EXECUTE_PROCESS(OUTPUT_VARIABLE __tmp_
            COMMAND which dh_builddeb
            RESULT_VARIABLE __dh_builddeb_check_result_
        )

        IF (__fakeroot_check_result_ EQUAL 0 AND __dh_builddeb_check_result_ EQUAL 0)
            SET(INSTALLATION_DIR "${CMAKE_CURRENT_BINARY_DIR}/debian/tmp")

            ADD_CUSTOM_COMMAND_EX(OUTPUT "${ARCADIA_BUILD_ROOT}/${PROJECTNAME}.deb"
                COMMAND cmake -DCMAKE_INSTALL_PREFIX="${INSTALLATION_DIR}" -P "${ARCADIA_BUILD_ROOT}/${prjdir}/cmake_install.cmake"
                COMMAND cp "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog" "${CMAKE_CURRENT_BINARY_DIR}/debian"
                COMMAND cp "${CMAKE_CURRENT_SOURCE_DIR}/debian/control" "${CMAKE_CURRENT_BINARY_DIR}/debian"
                COMMAND fakeroot dh_testdir
                COMMAND fakeroot dh_testroot
                COMMAND fakeroot dh_installchangelogs
                COMMAND fakeroot dh_installdocs
                COMMAND fakeroot dh_installexamples
                COMMAND fakeroot -- dh_install  --sourcedir=${INSTALLATION_DIR}
                COMMAND fakeroot dh_installman
                COMMAND fakeroot dh_link
                COMMAND fakeroot dh_strip
                COMMAND fakeroot dh_compress
                COMMAND fakeroot dh_fixperms
                COMMAND fakeroot dh_makeshlibs -V
                COMMAND fakeroot dh_installdeb
                COMMAND fakeroot dh_shlibdeps
                COMMAND fakeroot dh_gencontrol
                COMMAND fakeroot dh_md5sums
                COMMAND fakeroot -- dh_builddeb --destdir="${ARCADIA_BUILD_ROOT}" --filename=${PROJECTNAME}.deb
                DEPENDS
                        "${prjdir}/${PROJECTNAME}"
                        "${CMAKE_CURRENT_SOURCE_DIR}/debian/changelog"
                        "${CMAKE_CURRENT_SOURCE_DIR}/debian/control"
                        ${ARGV}
                COMMENT "Building ${PROJECTNAME} debian package"
            )

            ADD_EXECUTABLE_FICT(${PROJECTNAME}_package "${ARCADIA_BUILD_ROOT}/${PROJECTNAME}.deb")
        ELSE (__fakeroot_check_result_ EQUAL 0 AND __dh_builddeb_check_result_ EQUAL 0)
            IF (NOT __fakeroot_check_result_ EQUAL 0)
                MESSAGE(SEND_ERROR "MAKE_PACKAGE macro: fakeroot not found")
            ENDIF (NOT __fakeroot_check_result_ EQUAL 0)
            IF (NOT __dh_builddeb_check_result_ EQUAL 0)
                MESSAGE(SEND_ERROR "MAKE_PACKAGE macro: debhelper not found")
            ENDIF (NOT __dh_builddeb_check_result_ EQUAL 0)
            MESSAGE(SEND_ERROR "MAKE_PACKAGE macro: both fakeroot and debhelper needed to build debian packages")
        ENDIF (__fakeroot_check_result_ EQUAL 0 AND __dh_builddeb_check_result_ EQUAL 0)
    ENDIF (WIN32)
    LEAVE_PROJECT()
ENDMACRO(MAKE_PACKAGE)

