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

################################################################################
# Specify available build options.

option(YT_BUILD_EXPERIMENTS "Build experiments" TRUE)
option(YT_BUILD_TESTS "Build tests" TRUE)
option(YT_BUILD_WITH_STLPORT "Build with STLport" TRUE)

################################################################################
# Configure compilation flags.

# First, provide an ability to specify custom comilation flags.
# Initially take these flags either from environment or from special variable.

set( USER_C_FLAGS
  $ENV{CFLAGS} ${USER_CMAKE_C_FLAGS}
  CACHE STRING "User-defined C compiler flags")

set( USER_CXX_FLAGS
  $ENV{CXXFLAGS} ${USER_CMAKE_CXX_FLAGS}
  CACHE STRING "User-defined C++ compiler flags")

# Now configure compiler options for g++.
if (CMAKE_COMPILER_IS_GNUCXX)
  # These are default (basic) compilation flags.
  set( CMAKE_C_FLAGS "${USER_C_FLAGS} -pthread" )
  set( CMAKE_CXX_FLAGS "${USER_CXX_FLAGS} -std=gnu++0x -pthread" )
  
  # These are configuration-specific compilation flags.
  set( CMAKE_CXX_FLAGS_DEBUG "-g -O0" )
  set( CMAKE_CXX_FLAGS_RELEASE "-O2" )
  set( CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g -O2" )
  set( CMAKE_CXX_FLAGS_MINSIZEREL "-g -Os" )

  set( CMAKE_C_FLAGS_DEBUG "-g -O0" )
  set( CMAKE_C_FLAGS_RELEASE "-O2" )
  set( CMAKE_C_FLAGS_RELWITHDEBINFO "-g -O2" )
  set( CMAKE_C_FLAGS_MINSIZEREL "-g -Os" )
endif()

# Now configure compiler options for msvc.
if (MSVC)
  # These are default (basic) compilation flags.
  set( CMAKE_C_FLAGS "${USER_C_FLAGS}" )
  set( CMAKE_CXX_FLAGS "${USER_CXX_FLAGS} /EHsc" )

  # These are configuration-specific compliation flags.
  set( CMAKE_CXX_FLAGS_DEBUG "/Zi /Od" )
  set( CMAKE_CXX_FLAGS_RELEASE "/O2" )
  set( CMAKE_CXX_FLAGS_RELWITHDEBINFO "/Zi /O2" )
  set( CMAKE_CXX_FLAGS_MINSIZEREL "/O1" )

  set( CMAKE_C_FLAGS_DEBUG "/Zi /Od" )
  set( CMAKE_C_FLAGS_RELEASE "/O2" )
  set( CMAKE_C_FLAGS_RELWITHDEBINFO "/Zi /O2" )
  set( CMAKE_C_FLAGS_MINSIZEREL "/O1" )

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
  include_directories("${CMAKE_SOURCE_DIR}/extern/STLport/stlport")
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
