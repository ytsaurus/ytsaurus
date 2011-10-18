cmake_minimum_required(VERSION 2.6 FATAL_ERROR)
project(Yt)

################################################################################
# Include various CMake modules.

set(CMAKE_MODULE_PATH
    "${CMAKE_SOURCE_DIR}/cmake"
)

include(CMakeDependentOption)
include(CheckCXXCompilerFlag)
include(DumpCMakeVariables)

################################################################################
# Handy variables.

set(YT_ROOT ${YT_SOURCE_DIR})
set(YT_WORK ${YT_BINARY_DIR})

################################################################################
# Enforce developer to specify build type.

if (CMAKE_BUILD_TYPE STREQUAL "")
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

    check_cxx_compiler_flag(-std=c++0x GCC_CXX0X)
    if (NOT GCC_CXX0X)
        message(FATAL_ERROR "g++ lacks support of -std=c++0x")
    endif()

    check_cxx_compiler_flag(-fvisibility=hidden GCC_VISIBILITY)
    if (NOT GCC_VISIBILITY)
        message(FATAL_ERROR "g++ lacks support of -fvisibility=hidden")
    endif()

    add_definitions(-std=c++0x)
    add_definitions(-fvisibility=hidden)
    add_definitions(-fvisibility-inlines-hidden)

    set(XCODE_ATTRIBUTE_GCC_SYMBOLS_PRIVATE_EXTERN "YES")
    set(XCODE_ATTRIBUTE_GCC_INLINES_ARE_PRIVATE_EXTERN "YES")
endif()

################################################################################
# Configure build.

option(YT_BUILD_EXPERIMENTS "Build experiments" TRUE)
option(YT_BUILD_TESTS "Build tests" TRUE)

################################################################################
# Setup targets.

add_subdirectory(yt)
