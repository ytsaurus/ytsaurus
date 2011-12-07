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

################################################################################
# Setup targets.

if (YT_BUILD_WITH_STLPORT)
  add_subdirectory(extern/STLport/build/cmake)
endif()

add_subdirectory(util-build)

add_subdirectory(contrib/libs/snappy)
add_subdirectory(contrib/libs/lz4)
add_subdirectory(contrib/libs/fastlz)
add_subdirectory(contrib/libs/minilzo)
add_subdirectory(contrib/libs/zlib)
add_subdirectory(contrib/yajl)
add_subdirectory(contrib)

add_subdirectory(quality/Misc)
add_subdirectory(quality/NetLiba)

add_subdirectory(library/json)

add_subdirectory(yt)
