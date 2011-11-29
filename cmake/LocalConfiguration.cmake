
################################################################################
# Enforce developer to specify build type.

if ("${CMAKE_BUILD_TYPE}" STREQUAL "")
    set(CMAKE_BUILD_TYPE "RelWithDebInfo" CACHE STRING "Choose the type of build, options are: None (CMAKE_CXX_FLAGS or CMAKE_C_FLAGS used) Debug Release RelWithDebInfo MinSizeRel." FORCE)
endif()

# Configure compiler options for msvc.
if (MSVC)
    message(STATUS "Looks like we're using msvc...")

    if (MSVC_VERSION LESS 1600)
        message(FATAL_ERROR "msvc >= 10.0 is mandatory due to C++11 usage")
    endif()
endif()

# Configure compiler options for g++.
if (CMAKE_COMPILER_IS_GNUCXX)
    message(STATUS "Looks like we're using gcc...")

    execute_process(
        COMMAND ${CMAKE_CXX_COMPILER} -dumpversion
        OUTPUT_VARIABLE GCC_VERSION
    )

    if (GCC_VERSION VERSION_LESS 4.5)
        message(FATAL_ERROR "g++ >= 4.5.0 is mandatory due to C++11 usage")
    endif()

    #check_cxx_compiler_flag(-std=c++0x GCC_CXX0X)
    #if (NOT GCC_CXX0X)
    #    message(FATAL_ERROR "g++ lacks support of -std=c++0x")
    #endif()

    #check_cxx_compiler_flag(-fvisibility=hidden GCC_VISIBILITY)
    #if (NOT GCC_VISIBILITY)
    #    message(FATAL_ERROR "g++ lacks support of -fvisibility=hidden")
    #endif()

    # add_definitions(-std=c++0x)
    #SET( CMAKE_CXX_FLAGS  "-std=c++0x"
    #     CACHE STRING "C++ compiler flags" FORCE )
    #add_definitions(-fvisibility=hidden)
    #add_definitions(-fvisibility-inlines-hidden)

    #set(XCODE_ATTRIBUTE_GCC_SYMBOLS_PRIVATE_EXTERN "YES")
    #set(XCODE_ATTRIBUTE_GCC_INLINES_ARE_PRIVATE_EXTERN "YES")
endif()

################################################################################
# Configure build.

option(YT_BUILD_EXPERIMENTS "Build experiments" TRUE)
option(YT_BUILD_TESTS "Build tests" TRUE)
option(YT_BUILD_WITH_STLPORT "Build with STLport" TRUE)

if (CMAKE_COMPILER_IS_GNUCXX)
  if ( "${CMAKE_BUILD_TYPE}" STREQUAL "" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS "-std=gnu++0x -pthread -I${CMAKE_SOURCE_DIR}/extern/STLport/stlport" CACHE STRING "C++ compiler flags" FORCE )
    else ()
      SET( CMAKE_CXX_FLAGS "-std=gnu++0x -pthread" CACHE STRING "C++ compiler flags" FORCE )
    endif()
    SET( CMAKE_C_FLAGS "-pthread" CACHE STRING "C compiler flags" FORCE )
  endif()

  if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Release" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS_RELEASE  "-std=gnu++0x -pthread -I${CMAKE_SOURCE_DIR}/extern/STLport/stlport" CACHE STRING "C++ compiler flags" FORCE )
    else ()
      SET( CMAKE_CXX_FLAGS_RELEASE  "-std=gnu++0x -pthread" CACHE STRING "C++ compiler flags" FORCE )
    endif()
    SET( CMAKE_C_FLAGS_RELEASE  "-pthread" CACHE STRING "C compiler flags" FORCE )
  endif()

  if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Debug" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS_DEBUG "-std=gnu++0x -pthread -I${CMAKE_SOURCE_DIR}/extern/STLport/stlport" CACHE STRING "C++ compiler flags" FORCE )
    else()
      SET( CMAKE_CXX_FLAGS_DEBUG "-std=gnu++0x -pthread" CACHE STRING "C++ compiler flags" FORCE )
    endif()
    SET( CMAKE_C_FLAGS_DEBUG "-pthread" CACHE STRING "C compiler flags" FORCE )
  endif()

  if ( "${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=gnu++0x -pthread -I${CMAKE_SOURCE_DIR}/extern/STLport/stlport" CACHE STRING "C++ compiler flags" FORCE )
    else()
      SET( CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=gnu++0x -pthread" CACHE STRING "C++ compiler flags" FORCE )
    endif()
    SET( CMAKE_C_FLAGS_RELWITHDEBINFO  "-pthread" CACHE STRING "C compiler flags" FORCE )
  endif()
endif()

if (MSVC)
  #if ( "${CMAKE_BUILD_TYPE}" STREQUAL "" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS "/I${CMAKE_SOURCE_DIR}/extern/STLport/stlport /EHsc" CACHE STRING "C++ compiler flags" FORCE )
    else()
      SET( CMAKE_CXX_FLAGS "/EHsc" CACHE STRING "C++ compiler flags" FORCE )
    endif()
  #endif()

  #if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Release" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS_RELEASE  "/I${CMAKE_SOURCE_DIR}/extern/STLport/stlport /EHsc" CACHE STRING "C++ compiler flags" FORCE )
    else()
      SET( CMAKE_CXX_FLAGS_RELEASE  "/EHsc" CACHE STRING "C++ compiler flags" FORCE )
    endif()
  #endif()

  #if ( "${CMAKE_BUILD_TYPE}" STREQUAL "Debug" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS_DEBUG "/I${CMAKE_SOURCE_DIR}/extern/STLport/stlport /EHsc" CACHE STRING "C++ compiler flags" FORCE )
    else()
      SET( CMAKE_CXX_FLAGS_DEBUG "/EHsc" CACHE STRING "C++ compiler flags" FORCE )
    endif()
  #endif()

  #if ( "${CMAKE_BUILD_TYPE}" STREQUAL "RelWithDebInfo" )
    if (YT_BUILD_WITH_STLPORT)
      SET( CMAKE_CXX_FLAGS_RELWITHDEBINFO "/I${CMAKE_SOURCE_DIR}/extern/STLport/stlport /EHsc" CACHE STRING "C++ compiler flags" FORCE )
    else()
      SET( CMAKE_CXX_FLAGS_RELWITHDEBINFO "/EHsc" CACHE STRING "C++ compiler flags" FORCE )
    endif()
  #endif()
endif()

if (UNIX)
  SET( LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/lib )
  SET( EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin )
endif()

if (WIN32)
  SET( LIBRARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin )
  SET( EXECUTABLE_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin )
endif()
