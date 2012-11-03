# Tunable parameters:
#   - ENV[CC]
#   - ENV[CXX]
#   - ENV[CFLAGS]
#   - ENV[CXXFLAGS]
#   - CUSTOM_CMAKE_C_FLAGS
#   - CUSTOM_CMAKE_CXX_FLAGS
#   - YT_BUILD_ENABLE_EXPERIMENTS
#   - YT_BUILD_ENABLE_TESTS
#   - YT_BUILD_WITH_STLPORT

#################################################################################
# Specify available build options.

option(YT_BUILD_ENABLE_EXPERIMENTS "Build experiments" TRUE)
option(YT_BUILD_ENABLE_TESTS       "Build tests" TRUE)
option(YT_BUILD_ENABLE_NODEJS      "Build NodeJS extensions" FALSE)
option(YT_BUILD_WITH_STLPORT       "Build with STLport" FALSE)

################################################################################
# Enforce developer to specify build type.

if ( "${CMAKE_BUILD_TYPE}" STREQUAL "" )
  set( CMAKE_BUILD_TYPE "Debug"
    CACHE STRING
    "Choose the type of build, options are: None (CMAKE_CXX_FLAGS or CMAKE_C_FLAGS are used) Debug Release RelWithDebInfo MinSizeRel."
    FORCE )
endif()

################################################################################
# Check available compiler version.

if (MSVC)
  message(STATUS "Looks like we are using msvc..." )

  if ( MSVC_VERSION LESS 1600 )
    message(FATAL_ERROR "msvc >= 10.0 is mandatory due to C++11 usage")
  endif()
endif()

if (CMAKE_COMPILER_IS_GNUCXX)
  message(STATUS "Looks like we are using gcc...")

  execute_process(
    COMMAND ${CMAKE_CXX_COMPILER} -dumpversion
    OUTPUT_VARIABLE GCC_VERSION
  )

  if ( GCC_VERSION VERSION_LESS 4.5 )
    message(FATAL_ERROR "g++ >= 4.5.0 is mandatory due to C++11 usage")
  endif()
endif()

if ("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
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
  CACHE STRING "User-defined C compiler flags")

set( CUSTOM_CMAKE_CXX_FLAGS
  $ENV{CXXFLAGS} ${USER_CMAKE_CXX_FLAGS}
  CACHE STRING "User-defined C++ compiler flags")

# Now configure compiler options for clang.
if (CMAKE_COMPILER_IS_CLANG)
   # These are default (basic) compilation flags.
  set( CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS} -pthread -fPIC"
    CACHE STRING "(Auto-generated) C compiler flags" FORCE)
  set( CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} -std=c++11 -pthread -fPIC"
    CACHE STRING "(Auto-generated) C++ compiler flags" FORCE)

  # These are configuration-specific compilation flags.
  # http://gcc.gnu.org/onlinedocs/gcc/Option-Summary.html
  set( CMAKE_CXX_FLAGS_DEBUG "-g -O0"
    CACHE STRING "" FORCE)
  set( CMAKE_CXX_FLAGS_RELEASE "-O2"
    CACHE STRING "" FORCE)
  set( CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2"
    CACHE STRING "" FORCE)
  set( CMAKE_CXX_FLAGS_MINSIZEREL "-g -Os"
    CACHE STRING "" FORCE)

  set( CMAKE_C_FLAGS_DEBUG "-g -O0"
    CACHE STRING "" FORCE)
  set( CMAKE_C_FLAGS_RELEASE "-O2"
    CACHE STRING "" FORCE)
  set( CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2"
    CACHE STRING "" FORCE)
  set( CMAKE_C_FLAGS_MINSIZEREL "-g -Os"
    CACHE STRING "" FORCE)

  set( CMAKE_EXE_LINKER_FLAGS_RELEASE "" )
  set( CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "" )
  set( CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "" )
# Now configure compiler options for g++.
elseif (CMAKE_COMPILER_IS_GNUCXX)
  # These are default (basic) compilation flags.
  set( CMAKE_C_FLAGS "${CUSTOM_CMAKE_C_FLAGS} -pthread -fPIC -Wall -Wno-sign-compare -Wno-parentheses"
    CACHE STRING "(Auto-generated) C compiler flags" FORCE)
  set( CMAKE_CXX_FLAGS "${CUSTOM_CMAKE_CXX_FLAGS} -std=gnu++0x -pthread -fPIC -Wall -Wno-sign-compare -Wno-parentheses"
    CACHE STRING "(Auto-generated) C++ compiler flags" FORCE)

  # These are configuration-specific compilation flags.
  # http://gcc.gnu.org/onlinedocs/gcc/Option-Summary.html
  # Note that inlined version of memcmp is not used due to performance regressions in GCC.
  # http://gcc.gnu.org/bugzilla/show_bug.cgi?id=43052
  set( ARCH_FLAGS "-march=native -msse -msse2 -msse3 -msse4 -msse4.1 -msse4.2 -fno-builtin-strcmp -fno-builtin-strncmp -fno-builtin-memcmp" )

  set( CMAKE_CXX_FLAGS_DEBUG "-g -O0 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)
  set( CMAKE_CXX_FLAGS_RELEASE "-O2 -flto ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)
  set( CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)
  set( CMAKE_CXX_FLAGS_MINSIZEREL "-g -Os ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)

  set( CMAKE_C_FLAGS_DEBUG "-g -O0 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)
  set( CMAKE_C_FLAGS_RELEASE "-O2 -flto ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)
  set( CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2 ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)
  set( CMAKE_C_FLAGS_MINSIZEREL "-g -Os ${ARCH_FLAGS}"
    CACHE STRING "" FORCE)

  set( CMAKE_EXE_LINKER_FLAGS_RELEASE "-fwhole-program" )
  set( CMAKE_EXE_LINKER_FLAGS_RELWITHDEBINFO "" )
  set( CMAKE_EXE_LINKER_FLAGS_MINSIZEREL "" )
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
if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Debug" )
  add_definitions(-DDEBUG)
else()
  add_definitions(-DNDEBUG)
endif()

if (YT_BUILD_WITH_STLPORT)
  include_directories("${CMAKE_SOURCE_DIR}/contrib/STLport/stlport")
endif()

#if (YT_WITH_VISIBILITY)
#  add_definitions(-fvisibility=hidden)
#  add_definitions(-fvisibility-inlines-hidden)
#  set(XCODE_ATTRIBUTE_GCC_SYMBOLS_PRIVATE_EXTERN "YES")
#  set(XCODE_ATTRIBUTE_GCC_INLINES_ARE_PRIVATE_EXTERN "YES")
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

set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH FALSE)
