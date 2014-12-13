# Tunable parameters:
#   - ENV[CC]
#   - ENV[CXX]
#   - ENV[CFLAGS]
#   - ENV[CXXFLAGS]
#   - CUSTOM_CMAKE_C_FLAGS
#   - CUSTOM_CMAKE_CXX_FLAGS

################################################################################
# Enforce developer to specify build type.

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
  set( CMAKE_BUILD_TYPE "Debug"
    CACHE STRING
    "Choose the type of build, options are: None (CMAKE_CXX_FLAGS or CMAKE_C_FLAGS are used) Debug Release RelWithDebInfo MinSizeRel."
    FORCE )
endif()

################################################################################
# Check available compiler version.

if (MSVC)
  message(STATUS "Looks like we are using msvc..." )

  if (MSVC_VERSION LESS 1600)
    message(FATAL_ERROR "msvc >= 10.0 is mandatory due to C++11 usage")
  endif()
endif()

if (CMAKE_COMPILER_IS_GNUCXX)
  message(STATUS "Looks like we are using gcc...")

  if (CMAKE_CXX_COMPILER_ARG1)
    # XXX(psushin): Workaround for ccache.
    string(REPLACE " " "" CMAKE_CXX_COMPILER_ARG1 ${CMAKE_CXX_COMPILER_ARG1})
    execute_process( 
      COMMAND ${CMAKE_CXX_COMPILER_ARG1} -dumpversion 
      OUTPUT_VARIABLE GCC_VERSION
    )
  else()
    execute_process(
      COMMAND ${CMAKE_CXX_COMPILER} -dumpversion
      OUTPUT_VARIABLE GCC_VERSION
    )
  endif()

  if (GCC_VERSION VERSION_LESS 4.7)
    message(FATAL_ERROR "g++ >= 4.7.0 is mandatory")
  endif()
endif()

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
  message(STATUS "Looks like we are using clang...")

  # XXX(sandello): It's a temporary solution; it works because Clang driver
  # is compatible to GCC.
  set( CMAKE_COMPILER_IS_GNUCXX TRUE )
  set( CMAKE_COMPILER_IS_CLANG TRUE )
endif()

################################################################################
# Configure compilation flags.

# First, provide an ability to specify custom comilation flags.
# Initially take these flags either from environment or from special variable.

set( CUSTOM_CMAKE_C_FLAGS
  $ENV{CFLAGS} ${USER_CMAKE_C_FLAGS}
  CACHE STRING "User-defined C compiler flags" )

set( CUSTOM_CMAKE_CXX_FLAGS
  $ENV{CXXFLAGS} ${USER_CMAKE_CXX_FLAGS}
  CACHE STRING "User-defined C++ compiler flags" )

# Now configure compiler options for clang.
if (CMAKE_COMPILER_IS_CLANG)
   # These are default (basic) compilation flags.
  set( CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS} -pthread -fPIC"
    CACHE STRING "(Auto-generated) C compiler flags" FORCE )
  set( CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} -std=c++11 -pthread -fPIC"
    CACHE STRING "(Auto-generated) C++ compiler flags" FORCE )

  # Use libc++ instead of libstdc++ under Mac OS X, otherwise stick with libstdc++.
  if (APPLE)
    set ( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -stdlib=libc++" )
  endif()

  # These are configuration-specific compilation flags.
  # http://gcc.gnu.org/onlinedocs/gcc/Option-Summary.html
  set( CMAKE_CXX_FLAGS_DEBUG "-g -O0"
    CACHE STRING "" FORCE )
  set( CMAKE_CXX_FLAGS_RELEASE "-O2"
    CACHE STRING "" FORCE )
  set( CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2"
    CACHE STRING "" FORCE )
  set( CMAKE_CXX_FLAGS_MINSIZEREL "-g -Os"
    CACHE STRING "" FORCE )

  set( CMAKE_C_FLAGS_DEBUG "-g -O0"
    CACHE STRING "" FORCE )
  set( CMAKE_C_FLAGS_RELEASE "-O2"
    CACHE STRING "" FORCE )
  set( CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2"
    CACHE STRING "" FORCE )
  set( CMAKE_C_FLAGS_MINSIZEREL "-g -Os"
    CACHE STRING "" FORCE )

  set( CMAKE_EXE_LINKER_FLAGS_RELEASE "" )
  set( CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "" )
  set( CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "" )
# Now configure compiler options for g++.
elseif (CMAKE_COMPILER_IS_GNUCXX)
  # These are default (basic) compilation flags.
  set( WARNINGS_FLAGS "-Wall" )
  set( WARNINGS_FLAGS "${WARNINGS_FLAGS} -Wno-sign-compare -Wno-parentheses -Wno-unused-local-typedefs" )

  set( CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS} -pthread -fPIC -Wall ${WARNINGS_FLAGS}"
    CACHE STRING "(Auto-generated) C compiler flags" FORCE )
  set( CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} -std=gnu++0x -pthread -fPIC ${WARNINGS_FLAGS}"
    CACHE STRING "(Auto-generated) C++ compiler flags" FORCE )

  # These are configuration-specific compilation flags.
  # http://gcc.gnu.org/onlinedocs/gcc/Option-Summary.html
  # Note that inlined version of memcmp is not used due to performance regressions in GCC.
  # http://gcc.gnu.org/bugzilla/show_bug.cgi?id=43052
  # http://gcc.gnu.org/onlinedocs/gcc-4.1.2/gcc/Other-Builtins.html
  set( ARCH_FLAGS "-march=native -msse -msse2 -msse3 -msse4 -msse4.1 -msse4.2 -mno-avx -mpclmul" )
  set( ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-memcmp  -fno-builtin-memcpy  -fno-builtin-memset")
  set( ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strcat  -fno-builtin-strchr  -fno-builtin-strcmp")
  set( ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strcpy  -fno-builtin-strcspn -fno-builtin-strlen")
  set( ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strncat -fno-builtin-strncmp -fno-builtin-strncpy")
  set( ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strpbrk -fno-builtin-strrchr -fno-builtin-strspn")
  set( ARCH_FLAGS "${ARCH_FLAGS} -fno-builtin-strstr")

  set( CMAKE_CXX_FLAGS_DEBUG "-g -O0 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )
  set( CMAKE_CXX_FLAGS_RELEASE "-O2 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )
  set( CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )
  set( CMAKE_CXX_FLAGS_MINSIZEREL "-g -Os ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )

  set( CMAKE_C_FLAGS_DEBUG "-g -O0 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )
  set( CMAKE_C_FLAGS_RELEASE "-O2 -flto ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )
  set( CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2 -flto ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )
  set( CMAKE_C_FLAGS_MINSIZEREL "-g -Os -flto ${ARCH_FLAGS}"
    CACHE STRING "" FORCE )

  # TODO(sandello): Enable this when gcc will be stable.
  # set( CMAKE_EXE_LINKER_FLAGS_RELEASE "-fwhole-program" )
  # set( CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "-fwhole-program" )
  # set( CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "" )
endif()

# Now configure compiler options for msvc.
if (MSVC)
  # These are default (basic) compilation flags.
  set( CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS}" )
  set( CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} /Gd /EHsc" )

  # These are configuration-specific compliation flags.
  # http://msdn.microsoft.com/en-us/library/fwkeyyhe.aspx
  set( CMAKE_CXX_FLAGS_DEBUG "/Zi /Od /Oy- /GS" )
  set( CMAKE_CXX_FLAGS_RELEASE "/O2 /Oi /Oy- /GL" )
  set( CMAKE_CXX_FLAGS_RELWITHDEBINFO "/Zi /O2 /Oi /Oy- /GL" )
  set( CMAKE_CXX_FLAGS_MINSIZEREL "/O1 /Oi /Oy- /GL" )

  set( CMAKE_C_FLAGS_DEBUG "/Zi /Od /Oy- /GS" )
  set( CMAKE_C_FLAGS_RELEASE "/O2 /Oi /Oy- /GL" )
  set( CMAKE_C_FLAGS_RELWITHDEBINFO "/Zi /O2 /Oi /Oy- /GL" )
  set( CMAKE_C_FLAGS_MINSIZEREL "/O1 /Oi /Oy- /GL" )

  set( CMAKE_EXE_LINKER_FLAGS_DEBUG "/DEBUG" )
  set( CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "/DEBUG" )
endif()

# Now configure platform-independent options.
if ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug")
  add_definitions(-DDEBUG)
else()
  add_definitions(-DNDEBUG)
endif()

# Explicitly request C99 format macroses.
if (UNIX)
  add_definitions(-D_GNU_SOURCE)
  add_definitions(-D__STDC_CONSTANT_MACROS)
  add_definitions(-D__STDC_FORMAT_MACROS)
  add_definitions(-D__STDC_LIMIT_MACROS)
endif()

#if (YT_WITH_VISIBILITY)
#  add_definitions(-fvisibility=hidden)
#  add_definitions(-fvisibility-inlines-hidden)
#  set( XCODE_ATTRIBUTE_GCC_SYMBOLS_PRIVATE_EXTERN "YES" )
#  set( XCODE_ATTRIBUTE_GCC_INLINES_ARE_PRIVATE_EXTERN "YES" )
#endif()

################################################################################
# Configure output paths.

if (UNIX)
  set( LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib )
  set( EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin )
endif()

if (WIN32)
  set( LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin )
  set( EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin )
endif()

set( CMAKE_SKIP_BUILD_RPATH FALSE )
set( CMAKE_BUILD_WITH_INSTALL_RPATH FALSE )
set( CMAKE_INSTALL_RPATH "" )
set( CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE )
