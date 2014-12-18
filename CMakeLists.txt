cmake_minimum_required(VERSION 2.6.4 FATAL_ERROR)
project(YT)

#################################################################################
# Specify available build options.

option(YT_BUILD_ENABLE_BENCHMARKS  "Build benchmarks" TRUE)
option(YT_BUILD_ENABLE_DRIVER      "Build driver" TRUE)
option(YT_BUILD_ENABLE_EXPERIMENTS "Build experiments" TRUE)
option(YT_BUILD_ENABLE_TESTS       "Build tests" TRUE)
option(YT_BUILD_ENABLE_NODEJS      "Build NodeJS bindings" FALSE)
option(YT_BUILD_ENABLE_PYTHON      "Build Python bindings" TRUE)
option(YT_BUILD_ENABLE_PERL        "Build Perl bindings" FALSE)
option(YT_BUILD_ENABLE_LLVM        "Build LLVM MCJIT" TRUE)
option(YT_BUILD_HAVE_RAGEL         "Do you have Ragel 6.8?" FALSE)
option(YT_BUILD_HAVE_BISON         "Do you have Bison 3.0?" FALSE)
option(YT_BUILD_YTCORE_SHARED      "Build ytcore as a shared lib" FALSE)
option(YT_USE_FOLDERS              "Enable solution folders in Visual Studio. Disable for Express versions." TRUE)
option(YT_USE_LTO                  "Enable link time optimizations." FALSE)

################################################################################
# Include various CMake modules.

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

include(CMakeDependentOption)
include(CMakeCInformation)
include(CMakeCXXInformation)

include(CheckCSourceCompiles)
include(CheckCXXSourceCompiles)
include(CheckIncludeFiles)
include(CheckFunctionExists)

include(DumpCMakeVariables)

include(LocalFunctions)
include(LocalConfiguration)
include(LocalXCode)

include(Version)
include(PCH)

################################################################################
# Create CompilationDatabase (build/compile_commands.json).

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

################################################################################
# Configure all the things!

message(STATUS "Configuring YT ${YT_VERSION}")

################################################################################
# Setup targets.

# Check for various includes.
check_include_files(cxxabi.h       HAVE_CXXABI_H)
check_include_files(dlfcn.h        HAVE_DLFCN_H)
check_include_files(execinfo.h     HAVE_EXECINFO_H)
check_include_files(pthread.h      HAVE_PTHREAD_H)
check_include_files(sys/types.h    HAVE_SYS_TYPES_H)
check_include_files(sys/ucontext.h HAVE_SYS_UCONTEXT_H)
check_include_files(ucontext.h     HAVE_UCONTEXT_H)
check_include_files(unistd.h       HAVE_UNISTD_H)

# XXX(sandello): libunwind support is broken.
# check_include_files(libunwind.h    HAVE_LIBUNWIND_H)
# check_include_files(unwind.h       HAVE_UNWIND_H)
# find_library(HAVE_LIBUNWIND unwind)

# For some reason this header cannot be reliably located with CMake.
if(CMAKE_COMPILER_IS_GNUCXX)
  set(HAVE_CXXABI_H 1)
endif()

include(PCFromUcontext)

# Write down configuration.
configure_file(
  ${CMAKE_SOURCE_DIR}/yt/config.h.in
  ${CMAKE_BINARY_DIR}/yt/config.h
)

# Discover Python world.
if(YT_BUILD_ENABLE_PYTHON)
  find_package(PythonInterp)
  find_package(PythonLibs)
  if(NOT PYTHONINTERP_FOUND)
    message(FATAL_ERROR "Could not find Python interpreter to build with YT_BUILD_ENABLE_PYTHON=TRUE")
  endif()
  if(NOT PYTHONLIBS_FOUND)
    message(FATAL_ERROR "Could not find Python libraries to build with YT_BUILD_ENABLE_PYTHON=TRUE")
  endif()
  message(STATUS "Using Python: ${PYTHON_EXECUTABLE} (-I${PYTHON_INCLUDE_DIR} -l${PYTHON_LIBRARY})")
endif()

# Discover Perl world.
if(YT_BUILD_ENABLE_PERL)
  find_package(Perl 5.14)
  find_package(PerlLibs 5.14)
  if(NOT PERL_FOUND)
    message(FATAL_ERROR "Could not find Perl interpreter to build with YT_BUILD_ENABLE_PERL=TRUE")
  endif()
  if(NOT PERLLIBS_FOUND)
    message(FATAL_ERROR "Could not find Perl libraries to build with YT_BUILD_ENABLE_PERL=TRUE")
  endif()
  message(STATUS "Using Perl: ${PERL_EXECUTABLE} (-I${PERL_INCLUDE_DIR} -l${PERL_LIBRARY})")
endif()

# Discover LLVM.
if(YT_BUILD_ENABLE_LLVM)
  # Using CMake module FindLLVM.cmake form <root>/cmake directory.
  find_package(LLVM)

  link_directories(${LLVM_LIBRARY_DIRS})
  include_directories(${LLVM_INCLUDE_DIRS})
  add_definitions(-DYT_USE_LLVM)
endif()

# Check for Ragel.
if(YT_BUILD_HAVE_RAGEL)
  find_program(RAGEL_EXECUTABLE ragel)
  mark_as_advanced(RAGEL_EXECUTABLE)
  if(NOT RAGEL_EXECUTABLE)
    message(FATAL_ERROR "Could not find Ragel")
  endif()
  execute_process(
    COMMAND ${RAGEL_EXECUTABLE} --version
    OUTPUT_VARIABLE RAGEL_VERSION
  )
  if(NOT RAGEL_VERSION MATCHES "6.8")
    message(FATAL_ERROR "Ragel 6.8 is required")
  endif()
  message(STATUS "Found Ragel: ${RAGEL_EXECUTABLE}")
endif()

# Check for Bison.
if(YT_BUILD_HAVE_BISON)
  find_program(BISON_EXECUTABLE bison)
  mark_as_advanced(BISON_EXECUTABLE)
  if(NOT BISON_EXECUTABLE)
    message(FATAL_ERROR "Could not find Bison")
  endif()
  execute_process(
    COMMAND ${BISON_EXECUTABLE} --version
    OUTPUT_VARIABLE BISON_VERSION
  )
  if(NOT BISON_VERSION MATCHES "3.0")
    message(FATAL_ERROR "Bison 3.0 is required")
  endif()
  message(STATUS "Found Bison: ${BISON_EXECUTABLE}")
endif()

# TODO(sandello): Check whether these two directories differ.
exec_program(
  ${CMAKE_COMMAND} ARGS -E copy_directory
  ${CMAKE_SOURCE_DIR}/debian
  ${CMAKE_BINARY_DIR}/debian
)

add_subdirectory(cmake/deps-arcadia-util)
add_subdirectory(cmake/deps-arcadia-library-blockcodecs)
add_subdirectory(cmake/deps-arcadia-library-lwtrace)
add_subdirectory(cmake/deps-arcadia-library-messagebus)
add_subdirectory(cmake/deps-protobuf)
add_subdirectory(cmake/deps-ev)
add_subdirectory(cmake/deps-z-lz-lzo)
add_subdirectory(cmake/deps-jerasure)
add_subdirectory(cmake/deps-yajl)

add_subdirectory(library/json)
add_subdirectory(library/httpserver)
add_subdirectory(library/lfalloc)

add_subdirectory(yt)

if (YT_BUILD_ENABLE_PERL)
  add_subdirectory(perl)
endif()

if(YT_BUILD_ENABLE_PYTHON)
  add_subdirectory(cmake/deps-pycxx)
endif()

add_custom_target( package touch package-stamp
  COMMAND cp -r ${CMAKE_SOURCE_DIR}/debian ${CMAKE_BINARY_DIR}/debian
  COMMAND mkdir -p ARTIFACTS
  COMMAND dch
    --distributor "yandex"
    --distribution "unstable"
    --newversion "${YT_VERSION}"
    --urgency "low"
    --force-distribution
    "Package version bump\; no source changes."
  COMMAND debuild
    -e SOURCE_DIR=${CMAKE_SOURCE_DIR}
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

add_custom_target(
  version
  touch version-stamp
  COMMAND echo "${YT_VERSION}" > ytversion
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Stamping YT..."
)

add_custom_target(
  install-debian-yandex-yt
  ${CMAKE_COMMAND}
    -DCOMPONENT=Unspecified
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt'..."
)

add_custom_target( install-debian-yandex-yt-messagebus-proxy
  ${CMAKE_COMMAND}
    -DCOMPONENT=messagebus_proxy
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt-messagebus-proxy'..."
)

add_custom_target( install-debian-yandex-yt-standalone
  ${CMAKE_COMMAND}
    -DCOMPONENT=standalone
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt-standalone'..."
)

add_custom_target( install-debian-yandex-yt-http-proxy
  ${CMAKE_COMMAND}
    -DCOMPONENT=nodejs
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt-http-proxy'..."
)

add_custom_target( install-debian-yandex-yt-perl
  ${CMAKE_COMMAND}
    -DCOMPONENT=perl
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt-perl'..."
)

if(YT_USE_FOLDERS)
  set_property(GLOBAL PROPERTY USE_FOLDERS ON)
endif()

# TODO(sandello): Move these properties closer to target definitions.
set_target_properties(ytext-arcadia-util PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-arcadia-library-blockcodecs PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-arcadia-library-lwtrace PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-arcadia-library-messagebus PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-httpserver PROPERTIES FOLDER "Contrib")
set_target_properties(protobuf PROPERTIES FOLDER "Contrib")
set_target_properties(protoc PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-ev PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-fastlz PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-lz4 PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-minilzo PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-quicklz PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-snappy PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-zlib PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-jerasure PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-lfalloc PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-lfalloc-impl PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-lfalloc-fake PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-lfalloc-fake-impl PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-yajl PROPERTIES FOLDER "Contrib")
set_target_properties(ytext-json PROPERTIES FOLDER "Contrib")

set_target_properties(install-debian-yandex-yt PROPERTIES FOLDER "Misc")
set_target_properties(install-debian-yandex-yt-http-proxy PROPERTIES FOLDER "Misc")
set_target_properties(install-debian-yandex-yt-perl PROPERTIES FOLDER "Misc")
set_target_properties(package PROPERTIES FOLDER "Misc")
set_target_properties(version PROPERTIES FOLDER "Misc")

set_target_properties(unittester PROPERTIES FOLDER "YT")
set_target_properties(ytcore PROPERTIES FOLDER "YT")
set_target_properties(yt PROPERTIES FOLDER "YT")
set_target_properties(ytlib PROPERTIES FOLDER "YT")
set_target_properties(ytserver PROPERTIES FOLDER "YT")
set_target_properties(ytserver_lib PROPERTIES FOLDER "YT")

