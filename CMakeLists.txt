cmake_minimum_required(VERSION 2.6.4 FATAL_ERROR)
project(Yt)

################################################################################
# Include various CMake modules.

set(CMAKE_MODULE_PATH
  "${CMAKE_SOURCE_DIR}/cmake"
)

include(CMakeDependentOption)
include(CheckCXXCompilerFlag)
include(FindPythonInterp)

include(DumpCMakeVariables)
include(LocalFunctions)
include(LocalConfiguration)
include(PCH)

################################################################################
# Setup targets.

if (YT_BUILD_WITH_STLPORT)
  add_subdirectory(contrib/STLport/build/cmake)
endif()

add_subdirectory(cmake/deps)
add_subdirectory(contrib/yajl)
add_subdirectory(library/json)

add_subdirectory(yt)
