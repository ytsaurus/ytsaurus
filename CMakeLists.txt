cmake_minimum_required(VERSION 2.6.4 FATAL_ERROR)
project(YT)

# TODO(sandello): Move this version-related stuff into a separate file.
set(YT_VERSION_MAJOR 0)
set(YT_VERSION_MINOR 1)
set(YT_VERSION_PATCH 1)

if(NOT YT_BUILD_NUMBER)
  set(YT_BUILD_NUMBER 0)
endif()

if(NOT YT_BUILD_TAG)
  set(YT_BUILD_TAG "local")
endif()

set(YT_VERSION "${YT_VERSION_MAJOR}.${YT_VERSION_MINOR}.${YT_VERSION_PATCH}")
set(YT_VERSION "${YT_VERSION}-${YT_BUILD_NUMBER}")
set(YT_VERSION "${YT_VERSION}+${YT_BUILD_TAG}")

message(STATUS "Configuring YT ${YT_VERSION}")

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

configure_file(
  ${CMAKE_SOURCE_DIR}/yt/build.h.in
  ${CMAKE_BINARY_DIR}/yt/build.h
)

# TODO(sandello): Check whether these two directories differ.
exec_program(
  ${CMAKE_COMMAND} ARGS -E copy_directory
  ${CMAKE_SOURCE_DIR}/debian
  ${CMAKE_BINARY_DIR}/debian
)

if (YT_BUILD_WITH_STLPORT)
  add_subdirectory(contrib/STLport/build/cmake)
endif()

add_subdirectory(cmake/deps)
add_subdirectory(contrib/yajl)
add_subdirectory(library/json)

add_subdirectory(yt)

add_custom_target( package touch package-stamp
  COMMAND mkdir -p ARTIFACTS
  COMMAND dch
    --distributor "yandex"
    --distribution "unstable"
    --newversion "${YT_VERSION}"
    --urgency "low"
    --force-distribution
    "Package version bump\; no source changes."
  COMMAND debuild
    --no-tgz-check
    --no-lintian
    --check-dirname-level 0
    -b -j8
  COMMAND mv
    ../yandex-yt_${YT_VERSION}*
    ARTIFACTS
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Packaging YT..."
)

add_custom_target( version touch version-stamp
  COMMAND echo "${YT_VERSION}" > ytversion
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Stamping YT..."
)

add_custom_target( install_core
  ${CMAKE_COMMAND}
    -DCOMPONENT=Unspecified
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing YT core..."
)

if (YT_BUILD_ENABLE_NODEJS)
  add_custom_target( install_nodejs
    ${CMAKE_COMMAND}
      -DCOMPONENT=nodejs
      -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
    COMMENT "Installing YT NodeJS bindings..."
  )
endif()

