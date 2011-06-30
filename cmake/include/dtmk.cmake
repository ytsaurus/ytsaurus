MACRO(EXEC_DTMK)

#This file was made from dt.mk

# ============================================================================ #
# Here are global build variables (you can override them in your local.cmake)

SET_IF_NOTSET(USE_THREADS yes)

SET_IF_NOTSET(DESTDIR ${ARCADIA_BUILD_ROOT}/ysite/install)

# End of global build variables list
# ============================================================================ #

# Creating symlink to sources
IF (PLATFORM_SUPPORTS_SYMLINKS AND NOT EXISTS "${BINDIR}/SRC" AND NOT NO_SRCLINK)
    DEBUGMESSAGE(3 "Creating symlink ${BINDIR}/SRC to ${CURDIR}")
    EXECUTE_PROCESS(
        COMMAND ln -sf ${CURDIR} SRC
        WORKING_DIRECTORY ${BINDIR}
        OUTPUT_QUIET
    )
ENDIF (PLATFORM_SUPPORTS_SYMLINKS AND NOT EXISTS "${BINDIR}/SRC" AND NOT NO_SRCLINK)

SET_IF_NOTSET(CMAKE_INSTALL_PREFIX ${DESTDIR})

# =========================================================================== #

SET(OBJDIRPREFIX    "/OBJDIRPREFIX-is-deprecated")
SET(OBJDIR_ARCADIA  "/OBJDIR_ARCADIA-is-deprecated")
SET(CANONICALOBJDIR "/CANONICALOBJDIR-is-deprecated")

SET_IF_NOTSET(PEERLIBS "") # peer libs for current target
SET_IF_NOTSET(PEERLDADD "")
SET_IF_NOTSET(OBJADD "")
SET_IF_NOTSET(MPROFLIB "")

# TODO: translate it when building unittests
#.if defined(LIB) && make(unittests) && $(LIB) != "yutil"
#PEERDIR += util
#.endif

IF (PROG AND NOT CREATEPROG)
    SET(CREATEPROG ${PROG})
ENDIF (PROG AND NOT CREATEPROG)

SET(DTMK_CFLAGS "${COMMON_CFLAGS}")
SET(DTMK_CXXFLAGS "${COMMON_CXXFLAGS}")

SET_APPEND(DTMK_I ${CMAKE_CURRENT_BINARY_DIR})

SET(LINK_WLBSTATIC_BEGIN)
SET(LINK_WLBSTATIC_END)
IF (LINK_STATIC_LIBS)
    SET(LINK_WLBSTATIC_BEGIN "-Wl,-Bstatic")
    SET(LINK_WLBSTATIC_END "-Wl,-Bdynamic")
ENDIF (LINK_STATIC_LIBS)

IF (WIN32)
IF (NOT NOSSE)
    IF ("${HARDWARE_TYPE}" MATCHES "x86_64")
        SET_APPEND(DTMK_CFLAGS -DSSE_ENABLED=1 -DSSE2_ENABLED=1 -DSSE3_ENABLED=1)
    ELSE ("${HARDWARE_TYPE}" MATCHES "x86_64")
        IF (IS_MSSE2_SUPPORTED)
            SET_APPEND(DTMK_CFLAGS /arch:SSE2 -DSSE_ENABLED=1 -DSSE2_ENABLED=1)
        ELSEIF (IS_MSSE_SUPPORTED)
            SET_APPEND(DTMK_CFLAGS /arch:SSE -DSSE_ENABLED=1)
        ENDIF (IS_MSSE2_SUPPORTED)
    ENDIF ("${HARDWARE_TYPE}" MATCHES "x86_64")
ENDIF (NOT NOSSE)
ELSE (WIN32)
SET_APPEND(DTMK_CFLAGS -fexceptions)
IF (NOSSE)
    IF (IS_MNOSSE_SUPPORTED)
        SET_APPEND(DTMK_CFLAGS -mno-sse)
    ENDIF (IS_MNOSSE_SUPPORTED)
ELSE (NOSSE)
    IF (IS_MSSE_SUPPORTED)
        SET_APPEND(DTMK_CFLAGS -msse -DSSE_ENABLED=1)
    ENDIF (IS_MSSE_SUPPORTED)

    IF (IS_MSSE2_SUPPORTED)
        SET_APPEND(DTMK_CFLAGS -msse2 -DSSE2_ENABLED=1)
    ENDIF (IS_MSSE2_SUPPORTED)

    IF (IS_MSSSE3_SUPPORTED)
        SET_APPEND(DTMK_CFLAGS -mssse3 -DSSSE3_ENABLED=1)
    ENDIF (IS_MSSSE3_SUPPORTED)
ENDIF (NOSSE)
ENDIF (WIN32)


# c-flags-always
IF (WIN32)
    SET(CMAKE_CONFIGURATION_TYPES Debug Release)
    
    IF (CMAKE_SYSTEM_VERSION MATCHES "6.0")
        SET_IF_NOTSET(NTFLAG "_WIN32_WINNT=0x0600")
    ENDIF (CMAKE_SYSTEM_VERSION MATCHES "6.0")
    IF (CMAKE_SYSTEM_VERSION MATCHES "6.1")
        SET_IF_NOTSET(NTFLAG "_WIN32_WINNT=0x0601")
    ENDIF (CMAKE_SYSTEM_VERSION MATCHES "6.1")
    SET_IF_NOTSET(NTFLAG "_WIN32_WINNT=0x0501")
    
    SET(CMAKE_CXX_FLAGS "/DWIN32 /D_WINDOWS /DSTRICT /D_MBCS /D_CRT_SECURE_NO_WARNINGS /D_CRT_NONSTDC_NO_WARNINGS /D${NTFLAG} /Zm1000 /GR /nologo /c /Zi /FD /FC /EHsc /nologo /errorReport:prompt")
    SET_IF_NOTSET(WFLAG "/W4 /w34018 /w34265 /w34296 /w34431") #/w34640 /w34365
    IF (NO_COMPILER_WARNINGS)
        SET(WFLAG "/W0")
    ENDIF (NO_COMPILER_WARNINGS)
    SET(CMAKE_CXX_FLAGS_DEBUG   "/MTd /Ob0 /Od /D_DEBUG  /RTC1 ${WFLAG} ${CFLAGS_DEBUG}")
    SET(CMAKE_CXX_FLAGS_RELEASE "/MT /DNDEBUG /GF /Gy ${WFLAG} ${CFLAGS_RELEASE}")
    SET(OPTIMIZE "/Ox /Oi /Ob2")

    SET(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS}")
    SET(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
    SET(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")

    # Add "/DEBUG" for linker in Release build (i.e., Release = RelWithDebInfo)
    FOREACH(__item_ CMAKE_EXE_LINKER_FLAGS_RELEASE CMAKE_SHARED_LINKER_FLAGS_RELEASE)
        IF (NOT "${${__item_}}" MATCHES "/DEBUG")
            SET(${__item_} "${${__item_}} /DEBUG")
        ENDIF (NOT "${${__item_}}" MATCHES "/DEBUG")
    ENDFOREACH(__item_)

ELSE (WIN32)
    # No configuration types exist in make-environment
    # SET(CMAKE_CONFIGURATION_TYPES Debug Release)

    IF (NOT NO_DEBUGINFO)
        SET(__debug_info_flag_ "-g")
    ELSE (NOT NO_DEBUGINFO)
        SET(__debug_info_flag_ "")
    ENDIF (NOT NO_DEBUGINFO)

    SET(CMAKE_C_FLAGS            "-pipe ${__debug_info_flag_} -Wall -W -Wno-parentheses -D_GNU_SOURCE -D_FILE_OFFSET_BITS=64 -D_LARGEFILE_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -DGNU")
    SET(CMAKE_CXX_FLAGS          "${CMAKE_C_FLAGS} -Woverloaded-virtual")

    SET(CMAKE_CXX_FLAGS_RELEASE  "-O2 -DNDEBUG ${CFLAGS_RELEASE}")
    IF (NOT IS_PATHSCALE)
        IF (NOT NOGCCSTACKCHECK)
            SET(CMAKE_CXX_FLAGS_DEBUG    "-fstack-check -fstack-protector ${CFLAGS_DEBUG}")
        ELSE (NOT NOGCCSTACKCHECK)
            SET(CMAKE_CXX_FLAGS_DEBUG    "-fstack-protector ${CFLAGS_DEBUG}")
        ENDIF (NOT NOGCCSTACKCHECK)
    ENDIF (NOT IS_PATHSCALE)
    SET(CMAKE_CXX_FLAGS_VALGRIND "${CFLAGS_VALGRIND}")
    SET(CMAKE_CXX_FLAGS_PROFILE  "-O2 -DNDEBUG -pg ${CFLAGS_PROFILE}")
    SET(CMAKE_CXX_FLAGS_COVERAGE "-fprofile-arcs -ftest-coverage ${CFLAGS_COVERAGE}")

    SET(CMAKE_C_FLAGS_RELEASE    "${CMAKE_CXX_FLAGS_RELEASE}")
    SET(CMAKE_C_FLAGS_DEBUG      "${CMAKE_CXX_FLAGS_DEBUG}")
    SET(CMAKE_C_FLAGS_VALGRIND   "${CMAKE_CXX_FLAGS_VALGRIND}")
    SET(CMAKE_C_FLAGS_PROFILE    "${CMAKE_CXX_FLAGS_PROFILE}")
    SET(CMAKE_C_FLAGS_COVERAGE   "${CMAKE_CXX_FLAGS_COVERAGE}")

    # SET(CMAKE_EXE_LINKER_FLAGS_PROFILE    "-static -static-libgcc")
    # SET(CMAKE_SHARED_LINKER_FLAGS_PROFILE "-static -static-libgcc")
    IF (NO_RDYNAMIC)
        SET(CMAKE_SHARED_LIBRARY_LINK_CXX_FLAGS "")
        SET(CMAKE_SHARED_LIBRARY_LINK_C_FLAGS "")
    ENDIF (NO_RDYNAMIC)

    # These instructions are important when you cross-compile your code,
    #   for example using distcc (from 32bit to 64bit and vice versa)
    IF (NOT SUN AND NOT "${HARDWARE_TYPE}" MATCHES "mips" AND NOT "${HARDWARE_TYPE}" MATCHES "ia64" AND NOT "${HARDWARE_TYPE}" MATCHES "x86_64")
        SET_APPEND(DTMK_CFLAGS -m32)
    ELSEIF ("${HARDWARE_TYPE}" MATCHES "ia64" OR "${HARDWARE_TYPE}" MATCHES "x86_64")
        SET_APPEND(DTMK_CFLAGS -m64)
    ENDIF (NOT SUN AND NOT "${HARDWARE_TYPE}" MATCHES "mips" AND NOT "${HARDWARE_TYPE}" MATCHES "ia64" AND NOT "${HARDWARE_TYPE}" MATCHES "x86_64")
ENDIF (WIN32)

IF (${CCVERS} GREATER 30000)
    SET_APPEND(DTMK_CXXFLAGS -Wno-deprecated)
    #SET_APPEND(MDFLAGS -MP) # no need for gcc depends
ENDIF (${CCVERS} GREATER 30000)

IF (NOT IS_PATHSCALE)
    IF (NOT 30400 GREATER ${CCVERS})
        SET_APPEND(DTMK_CXXFLAGS -Wno-invalid-offsetof)
    ENDIF (NOT 30400 GREATER ${CCVERS})
ENDIF (NOT IS_PATHSCALE)

IF (${CCVERS} GREATER 40100 AND ${CCVERS} LESS 40200)
    SET_APPEND(DTMK_CXXFLAGS -Wno-strict-aliasing)
ENDIF (${CCVERS} GREATER 40100 AND ${CCVERS} LESS 40200)

IF (WERROR AND NOT NO_WERROR)
    IF (WIN32)
        SET_APPEND(DTMK_CFLAGS "-WX")
    ELSE (WIN32)
        SET_APPEND(DTMK_CFLAGS "-Werror")
    ENDIF (WIN32)
ENDIF (WERROR AND NOT NO_WERROR)

IF (NOT WIN32)
    IF (NO_COMPILER_WARNINGS)
        SET_APPEND(DTMK_CFLAGS "-w")
    ENDIF (NO_COMPILER_WARNINGS)
ENDIF (NOT WIN32)

SET_APPEND(DTMK_CFLAGS "${RKEYS}")

IF (NOT OPTIMIZE)
    IF (WIN32)
        SET_APPEND(OPTIMIZE /O2 /Ob2 /Ot)
    ELSE (WIN32)
        IF (NOT SUN AND NOT "${HARDWARE_TYPE}" MATCHES "ia64" AND NOT "${HARDWARE_TYPE}" MATCHES "x86_64")
            IF (${CCVERS} GREATER 30400)
                SET_APPEND(OPTIMIZE -march=pentiumpro -mtune=pentiumpro)
            ELSE (${CCVERS} GREATER 30400)
                SET_APPEND(OPTIMIZE -march=pentiumpro -mcpu=pentiumpro)
            ENDIF (${CCVERS} GREATER 30400)
        ENDIF (NOT SUN AND NOT "${HARDWARE_TYPE}" MATCHES "ia64" AND NOT "${HARDWARE_TYPE}" MATCHES "x86_64")
        IF (${CCVERS} LESS 40000)
            SET(OPTIMIZE ${OPTIMIZE} -O3 -DNDEBUG) #-finline-limit=40)
            IF (SPARC)
                SET_APPEND(OPTIMIZE -mcpu=ultrasparc)
            ENDIF (SPARC)
        ELSE (${CCVERS} LESS 40000)
            SET_APPEND(OPTIMIZE -O2 -DNDEBUG)
        ENDIF (${CCVERS} LESS 40000)
    ENDIF (WIN32)
ENDIF (NOT OPTIMIZE)

IF (NO_OPTIMIZE)
    IF (WIN32)
        SET(OPTIMIZE /Od)
    ELSE (WIN32)
        SET(OPTIMIZE -O0)
    ENDIF (WIN32)
ENDIF (NO_OPTIMIZE)

IF (NOT DEFINED NOOPTIMIZE)
    IF (WIN32)
        SET_APPEND(NOOPTIMIZE /Od)
    ELSE (WIN32)
        SET_APPEND(NOOPTIMIZE -O0)
    ENDIF (WIN32)
ENDIF (NOT DEFINED NOOPTIMIZE)

IF (SAVE_TEMPS AND NOT WIN32)
    SET_APPEND(DTMK_CFLAGS -save-temps)
ENDIF (SAVE_TEMPS AND NOT WIN32)

# =================================
# define Coverage and Profile build types

STRING(TOUPPER "${CMAKE_BUILD_TYPE}" CMAKE_BUILD_TYPE_UPPER)

IF ("${CMAKE_BUILD_TYPE}" MATCHES "Coverage" OR MAKE_COVERAGE)
    SET_APPEND(OBJADDE -lgcov)
ENDIF ("${CMAKE_BUILD_TYPE}" MATCHES "Coverage" OR MAKE_COVERAGE)

IF (DEFINED USEMPROF OR DEFINED USE_MPROF)
    IF (DEFINED SUN)
    ELSEIF (DEFINED LINUX)
        SET_APPEND(MPROFLIB -ldmalloc)
    ELSE  (DEFINED SUN)
        SET_APPEND(MPROFLIB -L/usr/local/lib -lc_mp)
        SET_APPEND(DTMK_D -DUSE_MPROF)
    ENDIF (DEFINED SUN)
ENDIF (DEFINED USEMPROF OR DEFINED USE_MPROF)

IF (DEFINED USE_THREADS AND "${USE_THREADS}" MATCHES "no")
    SET(USE_THREADS)
ENDIF (DEFINED USE_THREADS AND "${USE_THREADS}" MATCHES "no")

IF (USE_LIBBIND AND NOT NO_LIBBIND AND USE_THREADS)
    IF (FREEBSD_VER)
        SET_IF_NOTSET(BINDDIR /usr/local/bind-f4)
    ENDIF (FREEBSD_VER)
    IF (DEFINED BINDDIR AND NOT BINDDIR)
        SET(BINDDIR)
    ENDIF (DEFINED BINDDIR AND NOT BINDDIR)
    IF (BINDDIR)
        IF (NOT EXISTS ${BINDDIR}/include/bind/resolv.h)
            MESSAGE("dtmk.cmake warning: bind not found at ${BINDDIR}, use ENABLE(NO_LIBBIND) in local.cmake if libbind is not needed")
        ELSE (NOT EXISTS ${BINDDIR}/include/bind/resolv.h)
            SET_APPEND(OBJADDE ${LINK_WLBSTATIC_BEGIN} -L${BINDDIR}/lib -lbind_r ${LINK_WLBSTATIC_END})
            SET_APPEND(DTMK_D -DBIND_LIB)
            SET_APPEND(DTMK_I ${BINDDIR}/include/bind)
        ENDIF (NOT EXISTS ${BINDDIR}/include/bind/resolv.h)
    ENDIF (BINDDIR)
ENDIF (USE_LIBBIND AND NOT NO_LIBBIND AND USE_THREADS)

IF (USE_LIBBIND AND NO_LIBBIND)
    MESSAGE(STATUS "dtmk.cmake warning: NO_LIBBIND and USE_LIBBIND are positive. LIBBIND functionality is disabled since now. Please fix your project (${CURDIR})")
ENDIF (USE_LIBBIND AND NO_LIBBIND)

IF ("${CMAKE_BUILD_TYPE}" STREQUAL "valgrind" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Valgrind")
    SET (WITH_VALGRIND 1)
ENDIF ("${CMAKE_BUILD_TYPE}" STREQUAL "valgrind" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Valgrind")

IF (WITH_VALGRIND)
    SET_APPEND(DTMK_D -DWITH_VALGRIND=1)
    IF (EXISTS /usr/include/valgrind/valgrind.h AND EXISTS /usr/include/valgrind/memcheck.h)
        SET_APPEND(DTMK_D -DHAVE_VALGRIND=1)
    ENDIF (EXISTS /usr/include/valgrind/valgrind.h AND EXISTS /usr/include/valgrind/memcheck.h)
ENDIF (WITH_VALGRIND)

IF (USEMEMGUARD OR USE_MEMGUARD)
    IF (SUN)
        SET_APPEND(OBJADDE /usr/local/lib/libefence.a)
    ELSE (SUN)
        SET_APPEND(OBJADDE ${LINK_WLBSTATIC_BEGIN} -L/usr/local/lib -lefence ${LINK_WLBSTATIC_END})
    ENDIF (SUN)
ENDIF (USEMEMGUARD OR USE_MEMGUARD)

IF (NOT NOUTIL)
    DEBUGMESSAGE(3 "Appending util to PEERDIR (in ${CMAKE_CURRENT_SOURCE_DIR})")
    PEERDIR(util)
ENDIF (NOT NOUTIL)

IF (NOT "X${PROG}${CREATEPROG}X" STREQUAL "XX" AND NOT WITH_VALGRIND)
    IF (USE_LF_ALLOCATOR)
        DEBUGMESSAGE(3 "Appending util/private/lfalloc to PEERDIR (in ${CMAKE_CURRENT_SOURCE_DIR})")
        SET(USE_GOOGLE_ALLOCATOR no)
        SET(USE_J_ALLOCATOR no)

        PEERDIR(util/private/lfalloc)
    ENDIF (USE_LF_ALLOCATOR)

    IF (USE_J_ALLOCATOR)
        DEBUGMESSAGE(3 "Appending util/private/jalloc to PEERDIR (in ${CMAKE_CURRENT_SOURCE_DIR})")
        SET(USE_GOOGLE_ALLOCATOR no)

        IF (NOT WIN32)
            PEERDIR(util/private/jalloc)
        ENDIF (NOT WIN32)
    ENDIF (USE_J_ALLOCATOR)

    IF (USE_GOOGLE_ALLOCATOR)
        DEBUGMESSAGE(3 "Appending util/private/galloc to PEERDIR (in ${CMAKE_CURRENT_SOURCE_DIR})")
        PEERDIR(util/private/galloc)
    ENDIF (USE_GOOGLE_ALLOCATOR)
ELSE (NOT "X${PROG}${CREATEPROG}X" STREQUAL "XX" AND NOT WITH_VALGRIND)
    IF (PEERDIR MATCHES "util/.alloc")
        MESSAGE(SEND_ERROR "Error: PEERDIR at ${CURDIR} contains some allocator AND target is not an executable")
    ENDIF (PEERDIR MATCHES "util/.alloc")
ENDIF (NOT "X${PROG}${CREATEPROG}X" STREQUAL "XX" AND NOT WITH_VALGRIND)

INCLUDE(${ARCADIA_ROOT}/cmake/icc/icc.cmake)

IF (USE_ICC)
    IF (NOT "X${PROG}${CREATEPROG}X" STREQUAL "XX")
        PEERDIR(
            cmake/icc/src
        )
    ENDIF (NOT "X${PROG}${CREATEPROG}X" STREQUAL "XX")

    SRCDIR(
        ADDINCL cmake/icc/src
    )
ENDIF (USE_ICC)

SRCDIR(
    util/private/stl/stlport-${CURRENT_STLPORT_VERSION}
)

SET(USE_INTERNAL_STL yes)
IF (NOT USE_INTERNAL_STL)
    MESSAGE(STATUS "You've modified dtmk.cmake and set USE_INTERNAL_STL to negative value. If you get a compile error after it please ask pg@ what to do next")
    SET_APPEND(LDFLAGS -lstdc++)
ENDIF (NOT USE_INTERNAL_STL)

IF (USE_INTERNAL_STL AND NOT PROJECTNAME STREQUAL "private-stl")
    PEERDIR(
        util/private/stl
    )

    SRCDIR(
        util/private/stl/stlport-${CURRENT_STLPORT_VERSION}/stlport
    )

    CFLAGS(-DUSE_INTERNAL_STL)
ENDIF (USE_INTERNAL_STL AND NOT PROJECTNAME STREQUAL "private-stl")

IF (NOT WIN32)
    IF (USE_THREADS)
        #SET_APPEND(OBJADDE ${LINK_WLBSTATIC_BEGIN} ${THREADLIB} ${LINK_WLBSTATIC_END})
        SET_APPEND(OBJADDE ${THREADLIB})
        SET_APPEND(DTMK_D -D_THREAD_SAFE -D_PTHREADS -D_REENTRANT)
    ENDIF (USE_THREADS)
ENDIF (NOT WIN32)

IF (USE_BERKELEY OR USE_BERKELEYDB)
    SET_IF_NOTSET(BERKELEYDB /usr/local/db3.3)
    SET_IF_NOTSET(BERKELEYDB_INCLUDE ${BERKELEYDB}/include)
    SET_IF_NOTSET(BERKELEYDB_LIB -L${BERKELEYDB}/lib -ldb)
    SET_APPEND(DTMK_I ${BERKELEYDB_INCLUDE})
    SET_APPEND(THIRDPARTY_OBJADD ${BERKELEYDB_LIB})
ENDIF (USE_BERKELEY OR USE_BERKELEYDB)

IF (NOT LINUX AND NOT SUN)
    SET_IF_NOTSET(USE_ICONV yes)
ENDIF (NOT LINUX AND NOT SUN)

IF (USE_GNUTLS)
    SET_APPEND(DTMK_D -DUSE_GNUTLS)
    SET_APPEND(LIBS -L/usr/lib -lgnutls)
ENDIF (USE_GNUTLS)

IF (USE_BOOST)
    SET(BOOST_VER 1_31)
    SET_IF_NOTSET(BOOSTDIR /usr/local/boost)
    SET_IF_NOTSET(BOOSTINC ${BOOSTDIR}/include/boost-${BOOST_VER})
    SET_APPEND(DTMK_I ${BOOSTINC})
    SET_APPEND(OBJADDE -L${BOOSTDIR}/lib)
ENDIF (USE_BOOST)

IF (USE_LUA)
    SET_APPEND(DTMK_D -DUSE_LUA)
ENDIF (USE_LUA)

IF (USE_PYTHON)
    SET_APPEND(DTMK_D -DUSE_PYTHON)
#   SET_IF_NOTSET(PYTHON_VERSION 2.4)
ENDIF (USE_PYTHON)

IF ("${CMAKE_BUILD_TYPE}" STREQUAL "profile" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Profile" )
    ENABLE(PROFILE)
ENDIF ("${CMAKE_BUILD_TYPE}" STREQUAL "profile" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Profile" )

IF (USE_PERL)
    SET_APPEND(DTMK_D -DUSE_PERL)

    IF (WIN32)
        SET(PERLLIB ${PERLLIB_PATH}/perl512.lib)
        IF (NOT EXISTS ${PERLLIB})
            SET(PERLLIB ${PERLLIB_PATH}/perl510.lib)
        ENDIF (NOT EXISTS ${PERLLIB})
        IF (NOT EXISTS ${PERLLIB})
            SET(PERLLIB ${PERLLIB_PATH}/perl58.lib)
        ENDIF (NOT EXISTS ${PERLLIB})
        SET_APPEND(OBJADDE ${PERLLIB})
    ELSE (WIN32)
        IF ("${CMAKE_BUILD_TYPE}" STREQUAL "profile" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Profile" )
            SET_APPEND(OBJADDE -L${PERLLIB_PATH}/CORE -lperl_p)
            SET_APPEND(OBJADDE -lcrypt_p)
        ELSE ("${CMAKE_BUILD_TYPE}" STREQUAL "profile" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Profile" )
            SET_IF_NOTSET(PERLLIB_BIN "-lperl")
            IF (NOT SUN)
                IF (LINK_STATIC_PERL)
                    SET_APPEND(OBJADDE -Wl,-Bstatic -L${PERLLIB_PATH} ${PERLLIB_BIN} -lcrypt -Wl,-Bdynamic)
                ELSE (LINK_STATIC_PERL)
                    SET_APPEND(OBJADDE -L${PERLLIB_PATH} ${PERLLIB_BIN} -lcrypt -Wl,-E)
                ENDIF (LINK_STATIC_PERL)
            ELSE (NOT SUN)
                IF (SUN)
                    SET_APPEND(OBJADDE -Wl,-Bstatic -L${PERLLIB_PATH} ${PERLLIB_BIN} -Wl,-Bdynamic)
                ELSE (SUN)
                    SET_APPEND(OBJADDE -L${PERLLIB_PATH} ${PERLLIB_BIN})
                ENDIF (SUN)
                SET_APPEND(OBJADDE -lcrypt)
            ENDIF (NOT SUN)
        ENDIF ("${CMAKE_BUILD_TYPE}" STREQUAL "profile" OR "${CMAKE_BUILD_TYPE}" STREQUAL "Profile" )

        IF (50809 GREATER ${PERL_VERSION})
            SET_APPEND(THIRDPARTY_OBJADD ${PERLLIB}/auto/DynaLoader/DynaLoader.a)
        ENDIF (50809 GREATER ${PERL_VERSION})
    ENDIF (WIN32)
ENDIF (USE_PERL)

IF (SUN)
    SET_APPEND(DTMK_D -D_REENTRANT) # for errno
    SET_APPEND(DTMK_D -DMUST_ALIGN -DHAVE_PARAM_H)
    SET_APPEND(LIBS -lnsl -lsocket -ldl -lresolv)
ELSEIF (LINUX)
    SET_APPEND(LIBS -ldl)
ENDIF (SUN)

IF (ALLOCATOR)
    SET_APPEND(DTMK_D -D${ALLOCATOR})
ENDIF (ALLOCATOR)

MACRO(add_positive_define varname value)
    IF (NEED_${varname})
        ADD_DEFINITIONS(-DUSE_${varname}=${value})
    ENDIF (NEED_${varname})
ENDMACRO(add_positive_define)

add_positive_define(PERL_GENERATE 1)
add_positive_define(PERL_GOODWORDS 1)
add_positive_define(PERL_REQSTAT 1)
add_positive_define(PERL_HILITESTR 1)
add_positive_define(PERL_URL2CACHE 1)
add_positive_define(PERL_GEOTARGET 1)
add_positive_define(PERL_FILTER 1)
add_positive_define(PERL_MIRROR 1)

IF (ARCOS)
    ADD_DEFINITIONS(-D__ARC_OS__=1)
ENDIF (ARCOS)

# ============================================================================ #

SET(VARI "")

# ============================================================================ #

# Collecting additional include directories

SET(ADDSRC ${CMAKE_CURRENT_SOURCE_DIR})
# Current directory must be included to make relative includes work in generated files (like ragel),
# because files are generated in build directory.
# However, because of this, header names in arcadia must have no intesections with header names in /usr/include/
SET(ADDINCL ${CMAKE_CURRENT_SOURCE_DIR})

IF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/Makefile.cfg.cmake)
    ENABLE(LOCAL_MAKEFILE_CFG)
    DEBUGMESSAGE(3, "INCLUDE(\${CMAKE_CURRENT_SOURCE_DIR}/Makefile.cfg.cmake)")
    INCLUDE(${CMAKE_CURRENT_SOURCE_DIR}/Makefile.cfg.cmake)
    DISABLE(LOCAL_MAKEFILE_CFG)
ENDIF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/Makefile.cfg.cmake)

FOREACH(DIR ${ADDINCL})
    SET_APPEND(DTMK_I ${DIR})
ENDFOREACH(DIR)

FOREACH(DIR ${ADDSRC})
#.PATH: ${.CURDIR}/${DIR}
ENDFOREACH(DIR)

# TODO: another strange line
#DEPDIR ?=
SET(DEPDIR "")

IF (NOT NOPEER)
    IF (NOT LIB OR SHLIB_MAJOR)
        FOREACH(DIR ${PEERDIR})
            IF (EXISTS ${DIR})
                SET_APPEND(DEPDIR ${DIR})
            ENDIF (EXISTS ${DIR})
        ENDFOREACH(DIR)
    ENDIF (NOT LIB OR SHLIB_MAJOR)

    FOREACH (DIR ${TOOLDIR})
        IF (EXISTS ${DIR})
            SET_APPEND(TOOLDEPDIR ${DIR})
        ENDIF (EXISTS ${DIR})
    ENDFOREACH (DIR)
ENDIF (NOT NOPEER)


IF (DEFINED SHLIB_MAJOR)
    SET(TARGET_SHLIB_MAJOR ${SHLIB_MAJOR})
ENDIF (DEFINED SHLIB_MAJOR)
IF (DEFINED SHLIB_MINOR)
    SET(TARGET_SHLIB_MINOR ${SHLIB_MINOR})
ENDIF (DEFINED SHLIB_MINOR)


IF (NOT WIN32)
    IF (DEFINED TARGET_SHLIB_MAJOR)
        IF ("${HARDWARE_TYPE}" MATCHES "ia64" OR SUN)
            SET_APPEND(PICFLAGS -fPIC)
        ENDIF ("${HARDWARE_TYPE}" MATCHES "ia64" OR SUN)
    ENDIF (DEFINED TARGET_SHLIB_MAJOR)

    IF ("${HARDWARE_TYPE}" MATCHES "x86_64")
        SET_APPEND(PICFLAGS -fPIC)
    ENDIF ("${HARDWARE_TYPE}" MATCHES "x86_64")
ENDIF (NOT WIN32)

IF (MAKE_ONLY_SHARED_LIB AND NOT NO_SHARED_LIB)
    SET(MAKE_ONLY_SHARED_LIB0 yes)
    SET(DTMK_LIBTYPE SHARED)
ENDIF (MAKE_ONLY_SHARED_LIB AND NOT NO_SHARED_LIB)

IF (MAKE_ONLY_STATIC_LIB AND NOT NO_STATIC_LIB)
    SET(MAKE_ONLY_STATIC_LIB0 yes)
    SET(DTMK_LIBTYPE STATIC)
ENDIF (MAKE_ONLY_STATIC_LIB AND NOT NO_STATIC_LIB)

IF (MAKE_ONLY_STATIC_LIB0 AND MAKE_ONLY_SHARED_LIB0)
    MESSAGE(SEND_ERROR "MAKE_ONLY_SHARED_LIB and MAKE_ONLY_STATIC_LIB should not be used simultaneously (@ ${CMAKE_CURRENT_SOURCE_DIR})")
ENDIF (MAKE_ONLY_STATIC_LIB0 AND MAKE_ONLY_SHARED_LIB0)

# =============================================================================================================================== #
# Здесь добавляются для каждого рума
#    - корневая директория аркадии
#    - директория объектников аркадии.
#
#FOREACH (CR ${ROOMS})
#   SET_APPEND(DTMK_I ${CR}/arcadia)
#   SET_APPEND(DTMK_I ${OBJDIRPREFIX}/${CR}/arcadia)
#ENDFOREACH(CR)

# Заменяем это всё на:
SET_APPEND(DTMK_I ${ARCADIA_ROOT} ${ARCADIA_BUILD_ROOT})

IF (PEERDIR)
    LIST_LENGTH(PEERDIR_COUNT ${PEERDIR})

    SET_IF_NOTSET(LIBORDER_FILE "${ARCADIA_ROOT}/cmake/build/staticlibs.lorder")
    IF (0) # AND EXISTS "${LIBORDER_FILE}" AND NOT ${PEERDIR_COUNT} EQUAL 1)
        # Reorder PEERDIR according to LIBORDER_FILE's order
        EXECUTE_PROCESS(
            COMMAND echo "${PEERDIR}"
            COMMAND ${AWK} -v "peerdir=${PEERDIR}" -v statfile=${ARCADIA_ROOT}/cmake/build/alllibs -f ${ARCADIA_ROOT}/cmake/include/sortpeerlibs.awk "${LIBORDER_FILE}"
            OUTPUT_STRIP_TRAILING_WHITESPACE
            ERROR_STRIP_TRAILING_WHITESPACE
            OUTPUT_VARIABLE NEW_PEERDIR
            ERROR_VARIABLE PEERDIR_ABSENT
        )
        STRING(REGEX REPLACE "^ " "" NEW_PEERDIR "${NEW_PEERDIR}")

        IF ("${PEERDIR_ABSENT}" STREQUAL "")
            SEPARATE_ARGUMENTS(NEW_PEERDIR)
            DEBUGMESSAGE(3 "${PEERDIR} -> ${NEW_PEERDIR} (@ ${CMAKE_CURRENT_SOURCE_DIR})")
            SET(PEERDIR ${NEW_PEERDIR})
        ELSE ("${PEERDIR_ABSENT}" STREQUAL "")
            MESSAGE(1 "${PEERDIR}  ->  ${NEW_PEERDIR} (absent: ${PEERDIR_ABSENT}) (@ ${CMAKE_CURRENT_SOURCE_DIR})")
        ENDIF ("${PEERDIR_ABSENT}" STREQUAL "")
    ENDIF (0) # AND EXISTS "${LIBORDER_FILE}" AND NOT ${PEERDIR_COUNT} EQUAL 1)

    DEBUGMESSAGE(2 "PEERDIR[${PEERDIR}] in ${CURDIR}")
    FOREACH (DIR ${PEERDIR})
        SET(PEERPATH ${ARCADIA_ROOT}/${DIR})
        #SET_APPEND(DTMK_I ${ARCADIA_BUILD_ROOT}/${DIR})
        #SET(ADDINCL ${ARCADIA_ROOT}/${DIR})
        #GET_FILENAME_COMPONENT(LIBNAME ${DIR} NAME)
        SET(MAKE_ONLY_SHARED_LIB)
        SET(SHLIB_MAJOR)
        SET(SHLIB_MINOR)
        SET(CDEP "NOTFOUND")

        IF (NOT CDEP AND EXISTS ${PEERPATH})
            SET(CDEP ${PEERPATH})
        ENDIF ()

        IF (EXISTS ${CDEP}/Makefile.cfg.cmake)
            DEBUGMESSAGE(3, "INCLUDE(${CDEP}/Makefile.cfg.cmake) as PEERDIR's CDEP")
            INCLUDE(${CDEP}/Makefile.cfg.cmake)
        ENDIF (EXISTS ${CDEP}/Makefile.cfg.cmake)

        FOREACH (INC ${ADDINCL})
            # TODO: Check if it is ok
            SET_APPEND(DTMK_I ${INC})
        ENDFOREACH (INC)

        GET_GLOBAL_DIRNAME(__dir_ ${PEERPATH})

        # This check is a simple solution for METALIB-peerdir'ing-METALIB situations
        IS_METALIBRARY(__is_meta_ "${DIR}")
        IF (NOT __is_meta_)
            IF ("X${${__dir_}_DEPENDNAME_LIB}X" STREQUAL "XX")
                MESSAGE(SEND_ERROR "dtmk.cmake @ ${CURDIR}: PEERDIR LIBRARY in ${DIR} not found")
            ELSE ("X${${__dir_}_DEPENDNAME_LIB}X" STREQUAL "XX")
                GETTARGETNAME(__cur_lib_ ${${__dir_}_DEPENDNAME_LIB})
            ENDIF ("X${${__dir_}_DEPENDNAME_LIB}X" STREQUAL "XX")
            SET_APPEND(PEERLIBS "${__cur_lib_}")
            SET_APPEND(PEERDEPENDS ${${__dir_}_DEPENDNAME_LIB})
            GET_DIR_HAS_GENERATED(__dir_has_generated ${PEERPATH})
            IF (__dir_has_generated)
                ADD_DIR_DEP_GENERATED(${DIR})
            ELSE ()
                IMP_DIR_DEP_GENERATED(${PEERPATH})
            ENDIF ()
        ENDIF (NOT __is_meta_)
#        SET_APPEND(PEERLDADD "${${__dir_}_TARGETNAME_LIB}")
    ENDFOREACH (DIR)

    GET_DIR_DEP_GENERATED(__dir_dep_generated)
    IF (DEFINED __dir_dep_generated)
        LIST(LENGTH __dir_dep_generated __dir_dep_generated_len)
        IF ("0" LESS "${__dir_dep_generated_len}")
            BUILDAFTER(${__dir_dep_generated})
        ENDIF ()
    ENDIF ()

    DEBUGMESSAGE(3 "PEERLDADD ${PEERLDADD} in ${CMAKE_CURRENT_SOURCE_DIR}")
ENDIF (PEERDIR)

# TODO: make custom target for the unittests

#.if make(unittests) || make(clean)
#SRCDIR += unittest/lib
#SRCS1 != ls $(.CURDIR)/*_ut.cpp 2>&1 | grep -v "No such file or directory" | sed -e 's/.*\///'
#SRCS1 += utmain.cpp
#.if make(unittests)
#VARI =
#SRCS += $(SRCS1)
#OBJS = ${SRCS:N*.h:R:S/$/.o/g}
#CREATEPROG ?= $(LIB)
#.endif
#CLEANFILES += ${SRCS1:N*.h:R:S/$/.o/g} ${SRCS1:N*.h:R:S/$/.d/g}
#.if defined(LIB)
#CLEANFILES += $(LIB) $(.CURDIR)/$(LIB)
#.endif
#.endif

IF (SRCDIR)
    FOREACH (DIR ${SRCDIR})
        SET(ADDSRC .)
        SET(ADDINCL .)
        SET(CDEP "NOTFOUND")
        IF (NOT CDEP AND EXISTS ${ARCADIA_ROOT}/${DIR})
            SET(CDEP ${ARCADIA_ROOT}/${DIR})
            SET(CDEPOBJ ${ARCADIA_BUILD_ROOT}/${DIR})
        ENDIF (NOT CDEP AND EXISTS ${ARCADIA_ROOT}/${DIR})

        IF (EXISTS ${CDEP}/Makefile.cfg.cmake)
            DEBUGMESSAGE(3 "INCLUDE(${CDEP}/Makefile.cfg.cmake) as SRCDIR's CDEP")
            INCLUDE(${CDEP}/Makefile.cfg.cmake)
        ENDIF (EXISTS ${CDEP}/Makefile.cfg.cmake)

        IF (CDEP)
            FOREACH (INC ${ADDINCL})
                # TODO: Check if it is ok
                SET_APPEND(DTMK_I ${ARCADIA_ROOT}/${DIR}/${INC})
            ENDFOREACH (INC)

            FOREACH (SRC ${ADDSRC})
                # SRCDIR is processed in buildrules.cmake
                #.PATH: ${CDEP}/${SRC}
            ENDFOREACH (SRC)
        ENDIF (CDEP)

        IF (NOT DEFINED NO_OBJ_PATH)
            # SRCDIR is processed in buildrules.cmake
            #.PATH: ${ARCADIA_BUILD_ROOT}/${DIR}
        ENDIF (NOT DEFINED NO_OBJ_PATH)
    ENDFOREACH (DIR)
ENDIF (SRCDIR)

FOREACH (DIR ${SUBDIR})
    IF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})
        SET_APPEND(DEPDIR ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})
        SET_APPEND(SUBDIRS ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})
    ELSE (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})
        DEBUGMESSAGE(1 "${CMAKE_CURRENT_SOURCE_DIR}/${DIR} listed in \"${CMAKE_CURRENT_SOURCE_DIR}\"'s SUBDIR doesn't exist. Ignoring")
    ENDIF (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${DIR})
ENDFOREACH (DIR)

#.ifdef TRSS
#RSRS += ${TRSS:N*.h:R:S/$/.rsr/g}

#all release coverage check: $(RSRS)
#.endif

SEPARATE_ARGUMENTS_SPACE(PEERLDADD)

IF (SUN)
    SET(PEERLDADD "-z rescan ${PEERLDADD}")
ELSEIF (DARWIN)
    SET(PEERLDADD "${PEERLDADD} ${PEERLDADD}")
ELSEIF (WIN32)
    # PEERLDADD is already well-formatted
ELSE (SUN)
    IF (PEERLIBS AND NOT "${CMAKE_CXX_CREATE_SHARED_LIBRARY}" MATCHES "start-group <LINK_LIBRARIES>")
        STRING(REPLACE "<LINK_LIBRARIES>"   "-Wl,--start-group <LINK_LIBRARIES> -Wl,--end-group"   CMAKE_C_LINK_EXECUTABLE   "${CMAKE_C_LINK_EXECUTABLE}")
        STRING(REPLACE "<LINK_LIBRARIES>"   "-Wl,--start-group <LINK_LIBRARIES> -Wl,--end-group"   CMAKE_CXX_LINK_EXECUTABLE   "${CMAKE_CXX_LINK_EXECUTABLE}")

        STRING(REPLACE "<LINK_LIBRARIES>"   "-Wl,--start-group <LINK_LIBRARIES> -Wl,--end-group"   CMAKE_C_CREATE_SHARED_LIBRARY   "${CMAKE_C_CREATE_SHARED_LIBRARY}")
        STRING(REPLACE "<LINK_LIBRARIES>"   "-Wl,--start-group <LINK_LIBRARIES> -Wl,--end-group"   CMAKE_CXX_CREATE_SHARED_LIBRARY   "${CMAKE_CXX_CREATE_SHARED_LIBRARY}")
    ENDIF (PEERLIBS AND NOT "${CMAKE_CXX_CREATE_SHARED_LIBRARY}" MATCHES "start-group <LINK_LIBRARIES>")
ENDIF (SUN)

IF (SRCS)
    IF ("${CMAKE_BUILD_TYPE}" MATCHES "Coverage")
        SET_IF_NOTSET(GCOV_OUT_FNAME /dev/stdout)
        FOREACH (src_file ${SRCS})
            GET_FILENAME_COMPONENT(src_file_path ${src_file} PATH)
            GET_FILENAME_COMPONENT(src_file_name ${src_file} NAME_WE)
            GET_FILENAME_COMPONENT(src_file_ext ${src_file} EXT)

            # guarantee we have a relative path
            IF (src_file_path)
                STRING(REPLACE ${CMAKE_CURRENT_SOURCE_DIR} "" src_file_path ${src_file_path})
            ENDIF (src_file_path)

            SET_APPEND(ALLGCOV ${CMAKE_CURRENT_BINARY_DIR}/${src_file_path}/@@${src_file_name}${src_file_ext}.gcov)
            IF (EXISTS ${src_file_path}/${src_file_name}_c.gcno)
                ADD_CUSTOM_COMMAND_EX(
                    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${src_file_path}/@@${src_file_name}${src_file_ext}.gcov
                    PRE_BUILD
                    COMMAND echo "=====> SOURCE: " ${src_file} >>${GCOV_OUT_FNAME}
                    COMMAND echo "=====> GCOV OUT DIR: "${CMAKE_CURRENT_BINARY_DIR} >>${GCOV_OUT_FNAME}

                    COMMAND ${GCOV} ${GCOV_OPTIONS} -o ${src_file_path}/${src_file_name}_c.o ${src_file} >>${GCOV_OUT_FNAME} 2>&1
                    DEPENDS ${src_file_path}/${src_file_name}_c.gcno ${src_file_path}/${src_file_name}_c.gcda
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                    COMMENT ${GCOV} ${GCOV_OPTIONS} -o ${src_file_path}/${src_file_name}_c.o ${src_file}
                    )
            ELSE (EXISTS ${src_file_path}/${src_file_name}_c.gcno)
                ADD_CUSTOM_COMMAND_EX(
                    OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/${src_file_path}/@@${src_file_name}${src_file_ext}.gcov
                    PRE_BUILD
                    COMMAND echo Warning: ${src_file_path}/${src_file_name}_c.gcno not found
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                    )
            ENDIF (EXISTS ${src_file_path}/${src_file_name}_c.gcno)
        ENDFOREACH (src_file)

        ADD_CUSTOM_TARGET (gcov
            DEPENDS ${ALLGCOV}
        )

    ENDIF ("${CMAKE_BUILD_TYPE}" MATCHES "Coverage")

ENDIF (SRCS)

IF ("${CMAKE_BUILD_TYPE}" MATCHES "Release")
    IF (DEFINED PKG_FOR_FREEBSD_4.1)
        SET(PKG_FOR_FREEBSD_41_FLAGS -Wl,-u__ti9exception -lgcc)
    ELSE (DEFINED PKG_FOR_FREEBSD_4.1)
        SET(PKG_FOR_FREEBSD_41_FLAGS "")
    ENDIF (DEFINED PKG_FOR_FREEBSD_4.1)
ENDIF ("${CMAKE_BUILD_TYPE}" MATCHES "Release")

# ============================================================================ #
# Now parse CFLAGS and extract -I from there
SET(__cflags_)
FOREACH(__item_ ${CFLAGS})
    IF ("${__item_}" MATCHES "^-I")
        STRING(REGEX REPLACE "^-I" "" __dir_ "${__item_}")
        SET_APPEND(DTMK_I ${__dir_})
    ELSE ("${__item_}" MATCHES "^-I")
        SET_APPEND(__cflags_ ${__item_})
    ENDIF ("${__item_}" MATCHES "^-I")
ENDFOREACH(__item_)
SET(CFLAGS ${__cflags_})

# ============================================================================ #

# Now set cmake's C/C++ compiler flags
# === C Flags ===
SET_APPEND(DTMK_CFLAGS ${PICFLAGS} ${CFLAGS})
SET_APPEND(CMAKE_C_FLAGS ${DTMK_CFLAGS} ${CONLYFLAGS})

SET_APPEND(DTMK_CXXFLAGS ${CXXFLAGS})
SEPARATE_ARGUMENTS_SPACE(CMAKE_C_FLAGS)
SET_APPEND(CMAKE_C_FLAGS_RELEASE ${OPTIMIZE})
SEPARATE_ARGUMENTS_SPACE(CMAKE_C_FLAGS_RELEASE)
DEBUGMESSAGE(3 "${CURDIR}: C_FLAGS=${CMAKE_C_FLAGS}")
DEBUGMESSAGE(3 "${CURDIR}: C_FLAGS_RELEASE=${CMAKE_C_FLAGS_RELEASE}")

# === C++ Flags ===
SET_APPEND(CMAKE_CXX_FLAGS "${DTMK_CFLAGS} ${DTMK_CXXFLAGS}")
SET_APPEND(CMAKE_CXX_FLAGS_RELEASE ${OPTIMIZE})
SEPARATE_ARGUMENTS_SPACE(CMAKE_CXX_FLAGS)
SEPARATE_ARGUMENTS_SPACE(CMAKE_CXX_FLAGS_RELEASE)
DEBUGMESSAGE(3 "${CURDIR}: CXX_FLAGS=${CMAKE_CXX_FLAGS}")
DEBUGMESSAGE(3 "${CURDIR}: CXX_FLAGS_RELEASE=${CMAKE_CXX_FLAGS_RELEASE}")

# === Link Flags ===
SET(DTMK_L "${MPROFLIB} ${LDFLAGS} ${OBJADD} ${VAROBJS} ${LIBS} ${PKG_FOR_FREEBSD_41_FLAGS}") # ${PEERLDADD}
SET(CMAKE_EXE_LINKER_FLAGS "${PROFFLAG}")
IF(MSVC AND NOT USE_MANIFEST)
    SET_APPEND(CMAKE_EXE_LINKER_FLAGS " /MANIFEST:NO")
ENDIF(MSVC AND NOT USE_MANIFEST)
SEPARATE_ARGUMENTS_SPACE(CMAKE_EXE_LINKER_FLAGS)


# === Static C++ runtime ===
IF (USE_STATIC_CPP_RUNTIME)
    IF (NOT "X${ST_OBJADDE}${ST_LDFLAGS}X" MATCHES "XX")
        IF (NOT "${CMAKE_C_LINK_EXECUTABLE}" MATCHES "${ST_LDFLAGS}")
            STRING(REPLACE "<CMAKE_C_COMPILER>"   "<CMAKE_C_COMPILER> ${ST_LDFLAGS}"   CMAKE_C_LINK_EXECUTABLE   "${CMAKE_C_LINK_EXECUTABLE}")
            STRING(REPLACE "<CMAKE_CXX_COMPILER>" "<CMAKE_CXX_COMPILER> ${ST_LDFLAGS}" CMAKE_CXX_LINK_EXECUTABLE "${CMAKE_CXX_LINK_EXECUTABLE}")
        ENDIF (NOT "${CMAKE_C_LINK_EXECUTABLE}" MATCHES "${ST_LDFLAGS}")

        DEBUGMESSAGE(2 "${CURDIR}: CMAKE_C_LINK_EXECUTABLE=${CMAKE_C_LINK_EXECUTABLE}")
        DEBUGMESSAGE(2 "${CURDIR}: CMAKE_CXX_LINK_EXECUTABLE=${CMAKE_CXX_LINK_EXECUTABLE}")

        # Avoid replacement if run for the second time (in CREATE_SHARED_LIBRARY())"
        IF (NOT "${CMAKE_C_CREATE_SHARED_LIBRARY}" MATCHES "${ST_LDFLAGS}" AND USE_DL_STATIC_RUNTIME)
            STRING(REPLACE "<CMAKE_C_COMPILER>"   "<CMAKE_C_COMPILER> ${ST_LDFLAGS}"   CMAKE_C_CREATE_SHARED_LIBRARY   "${CMAKE_C_CREATE_SHARED_LIBRARY}")
            STRING(REPLACE "<CMAKE_CXX_COMPILER>" "<CMAKE_CXX_COMPILER> ${ST_LDFLAGS}" CMAKE_CXX_CREATE_SHARED_LIBRARY "${CMAKE_CXX_CREATE_SHARED_LIBRARY}")
        ENDIF (NOT "${CMAKE_C_CREATE_SHARED_LIBRARY}" MATCHES "${ST_LDFLAGS}" AND USE_DL_STATIC_RUNTIME)

        SET_APPEND(THIRDPARTY_OBJADD ${ST_OBJADDE})
    ENDIF (NOT "X${ST_OBJADDE}${ST_LDFLAGS}X" MATCHES "XX")
ENDIF (USE_STATIC_CPP_RUNTIME)

IF (NO_MAPREDUCE)
    ADD_DEFINITIONS(-DNO_MAPREDUCE)
ENDIF (NO_MAPREDUCE)

ENDMACRO(EXEC_DTMK)
