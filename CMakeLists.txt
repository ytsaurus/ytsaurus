cmake_minimum_required(VERSION 2.8.10 FATAL_ERROR)
if(POLICY CMP0037)
  cmake_policy(SET CMP0037 OLD) # Enable reserved "package" target
endif()
if(POLICY CMP0042)
  cmake_policy(SET CMP0042 NEW) # Set MACOSX_RPATH=YES by default
endif()

project(YT)

#################################################################################
# Specify available build options.

option(YT_BUILD_ENABLE_BENCHMARKS         "Build benchmarks" TRUE)
option(YT_BUILD_ENABLE_DRIVER             "Build driver" TRUE)
option(YT_BUILD_ENABLE_EXPERIMENTS        "Build experiments" TRUE)
option(YT_BUILD_ENABLE_TESTS              "Build tests" TRUE)
option(YT_BUILD_ENABLE_NODEJS             "Build NodeJS bindings" FALSE)
option(YT_BUILD_ENABLE_PYTHON             "Build Python bindings" TRUE)
option(YT_BUILD_ENABLE_PYTHON_SKYNET      "Build Python skynet bindings" FALSE)
option(YT_BUILD_ENABLE_PERL               "Build Perl bindings" FALSE)
option(YT_BUILD_HAVE_RAGEL                "Do you have Ragel 6.8?" FALSE)
option(YT_BUILD_HAVE_BISON                "Do you have Bison 3.0?" FALSE)
option(YT_BUILD_ENABLE_ASAN               "Build with Address Sanitizer enabled" FALSE)
option(YT_USE_LTO                         "Enable link time optimizations" FALSE)

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

include(DetectCPU)
include(DumpCMakeVariables)

include(LocalFunctions)
include(LocalConfiguration)

include(Version)

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

include(PCH)
include(PCFromUcontext)

# Using CMake module FindLLVM.cmake form <root>/cmake directory.
find_package(LLVM)

link_directories(${LLVM_LIBRARY_DIRS})
include_directories(${LLVM_INCLUDE_DIRS})

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

# Discover skynet python.
if(YT_BUILD_ENABLE_PYTHON_SKYNET)
  set(PYTHON_SKYNET_EXECUTABLE "/skynet/python/bin/python")
  set(PYTHON_SKYNET_INCLUDE_DIR "/skynet/python/include/python2.7")
  set(PYTHON_SKYNET_LIBRARY "/skynet/python/lib/libpython2.7.so")
  foreach(PYTHON_VAR ${PYTHON_SKYNET_EXECUTABLE} ${PYTHON_SKYNET_INCLUDE_DIR} ${PYTHON_SKYNET_LIBRARY})
      if (NOT EXISTS ${PYTHON_VAR})
        message(FATAL_ERROR "Failed to find Skynet Python: path ${PYTHON_VAR} does not exist")
      endif()
  endforeach(PYTHON_VAR)
  message(STATUS "Using Skynet Python: ${PYTHON_SKYNET_EXECUTABLE} (-I${PYTHON_SKYNET_INCLUDE_DIR} -l${PYTHON_SKYNET_LIBRARY})")
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

# Check for Ragel.
if(YT_BUILD_HAVE_RAGEL)
  find_program(RAGEL_EXECUTABLE ragel)
  mark_as_advanced(RAGEL_EXECUTABLE)
  if(NOT RAGEL_EXECUTABLE)
    message(FATAL_ERROR "Could not find Ragel")
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
  message(STATUS "Found Bison: ${BISON_EXECUTABLE}")
endif()

add_subdirectory(debian)
add_subdirectory(cmake/deps-protobuf)
add_subdirectory(cmake/deps-arcadia-util)
add_subdirectory(cmake/deps-arcadia-contrib-libs-re2)
add_subdirectory(cmake/deps-arcadia-contrib-libs-snappy)
add_subdirectory(cmake/deps-arcadia-contrib-libs-zlib)
add_subdirectory(cmake/deps-arcadia-library-blockcodecs)
add_subdirectory(cmake/deps-arcadia-library-getopt)
add_subdirectory(cmake/deps-arcadia-library-http)
add_subdirectory(cmake/deps-arcadia-library-lfalloc)
add_subdirectory(cmake/deps-arcadia-library-lwtrace)
add_subdirectory(cmake/deps-arcadia-library-malloc-api)
add_subdirectory(cmake/deps-arcadia-library-messagebus)
add_subdirectory(cmake/deps-arcadia-library-streams-lz)
add_subdirectory(cmake/deps-contrib-libs-brotli)
add_subdirectory(cmake/deps-yt-contrib-jerasure)
add_subdirectory(cmake/deps-yt-contrib-libev)
add_subdirectory(cmake/deps-yt-contrib-lz4)
add_subdirectory(cmake/deps-yt-contrib-farmhash)
add_subdirectory(cmake/deps-yt-contrib-fastlz)
add_subdirectory(cmake/deps-yt-contrib-minilzo)
add_subdirectory(cmake/deps-yt-contrib-quicklz)
add_subdirectory(cmake/deps-yt-contrib-zstd)
add_subdirectory(cmake/deps-yt-contrib-zstd-legacy)
add_subdirectory(cmake/deps-yajl)
add_subdirectory(cmake/deps-contrib-benchmark)
add_subdirectory(python)
add_subdirectory(yt)

if(YT_BUILD_ENABLE_PERL)
  add_subdirectory(perl)
endif()

if(YT_BUILD_ENABLE_PYTHON)
  add_subdirectory(cmake/deps-pycxx)
endif()

execute_process(COMMAND lsb_release --codename --short
                OUTPUT_VARIABLE LSB_RELEASE)
if(${LSB_RELEASE} MATCHES "trusty")
  set(DCH_VENDOR_FLAG "--vendor")
else()
  set(DCH_VENDOR_FLAG "--distributor")
endif()

add_custom_target( changelog touch changelog-stamp
  COMMAND cp -r ${CMAKE_SOURCE_DIR}/debian ${CMAKE_BINARY_DIR}/debian
  COMMAND mkdir -p ARTIFACTS
  COMMAND dch
    ${DCH_VENDOR_FLAG} "yandex"
    --distribution "unstable"
    --newversion "${YT_VERSION}"
    --urgency "low"
    --force-distribution
    "Package version bump\; no source changes."
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Updating changelog..."
)

add_custom_target( package touch package-stamp
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
  DEPENDS changelog
)

add_custom_target( python-package touch python-package-stamp
  COMMAND cat ${CMAKE_BINARY_DIR}/debian/changelog | sed 's/^yandex-yt/yandex-yt-python-driver/' > ${CMAKE_SOURCE_DIR}/python/yandex-yt-python-driver/debian/changelog
  COMMAND ${CMAKE_SOURCE_DIR}/python/build_driver.sh
  COMMAND mv
    ../yandex-yt*${YT_VERSION}*
    ${CMAKE_BINARY_DIR}/ARTIFACTS
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/python
  COMMENT "Packaging YT python driver..."
  DEPENDS changelog
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

add_custom_target( install-debian-yandex-yt-perl-${YT_ABI_VERSION}
  ${CMAKE_COMMAND}
    -DCOMPONENT=perl-abi
    -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
  COMMENT "Installing for package 'yandex-yt-perl-${YT_ABI_VERSION}'..."
)

