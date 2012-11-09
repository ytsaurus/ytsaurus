cmake_minimum_required(VERSION 2.6.4 FATAL_ERROR)
project(YT)

# set(YT_BUILD_ENABLE_NODEJS 1)

################################################################################
# Include various CMake modules.

set(CMAKE_MODULE_PATH
  "${CMAKE_SOURCE_DIR}/cmake"
)

include(CMakeDependentOption)

include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)
include(CheckCXXCompilerFlag)
include(CheckIncludeFiles)
include(CheckFunctionExists)

include(FindPythonInterp)

include(DumpCMakeVariables)

include(LocalFunctions)
include(LocalConfiguration)

include(Version)
include(PCH)

# TODO(sandello): We all have to migrate to a newer CMake aforehead.
# include(cotire)

message(STATUS "Configuring YT ${YT_VERSION}")

################################################################################
# Setup targets.

check_include_files(cxxabi.h       HAVE_CXXABI_H)
check_include_files(dlfcn.h        HAVE_DLFCN_H)
check_include_files(execinfo.h     HAVE_EXECINFO_H)
check_include_files(libunwind.h    HAVE_LIBUNWIND_H)
check_include_files(pthread.h      HAVE_PTHREAD_H)
check_include_files(sys/types.h    HAVE_SYS_TYPES_H)
check_include_files(sys/ucontext.h HAVE_SYS_UCONTEXT_H)
check_include_files(ucontext.h     HAVE_UCONTEXT_H)
check_include_files(unwind.h       HAVE_UNWIND_H)
check_include_files(unistd.h       HAVE_UNISTD_H)

include(PCFromUcontext)

find_library(HAVE_LIBUNWIND unwind)

# For some reason this header cannot be reliably located with CMake.
if (CMAKE_COMPILER_IS_GNUCXX)
  set(HAVE_CXXABI_H 1)
endif()

configure_file(
  ${CMAKE_SOURCE_DIR}/yt/build.h.in
  ${CMAKE_BINARY_DIR}/yt/build.h
)

configure_file(
  ${CMAKE_SOURCE_DIR}/yt/config.h.in
  ${CMAKE_BINARY_DIR}/yt/config.h
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

add_subdirectory(cmake/deps-arcadia-util)
add_subdirectory(cmake/deps-protobuf)
add_subdirectory(cmake/deps-uv)
add_subdirectory(cmake/deps-z-lz-lzo)
add_subdirectory(cmake/deps-pycxx)

add_subdirectory(contrib/yajl)
add_subdirectory(library/json)

add_subdirectory(yt)
add_subdirectory(python/yt/driver)

install(
  FILES scripts/colorize-log.py
  DESTINATION bin
  PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
  RENAME ycl
)

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
    -b
  COMMAND mv
    ../yandex-yt*${YT_VERSION}*
    ARTIFACTS
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Packaging YT..."
)

add_custom_target( version touch version-stamp
  COMMAND echo "${YT_VERSION}" > ytversion
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Stamping YT..."
)

add_custom_target( install-debian-yandex-yt
  ${CMAKE_COMMAND}
    -DCOMPONENT=Unspecified
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt'..."
)

add_custom_target( install-debian-yandex-yt-http-api
  ${CMAKE_COMMAND}
    -DCOMPONENT=nodejs
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt-http-api'..."
)
