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

option(YT_BUILD_ENABLE_SERVER_BINARIES    "Build ytserver-* binaries" TRUE)
option(YT_BUILD_ENABLE_BENCHMARKS         "Build benchmarks" TRUE)
option(YT_BUILD_ENABLE_EXPERIMENTS        "Build experiments" TRUE)
option(YT_BUILD_ENABLE_TESTS              "Build tests" TRUE)
option(YT_BUILD_ENABLE_NODEJS             "Build NodeJS bindings" FALSE)
option(YT_BUILD_ENABLE_PYTHON_2_6         "Build Python 2.6 bindings" FALSE)
option(YT_BUILD_ENABLE_PYTHON_2_7         "Build Python 2.7 bindings" TRUE)
option(YT_BUILD_ENABLE_PYTHON_SKYNET      "Build Python Skynet bindings" FALSE)
option(YT_BUILD_ENABLE_PYTHON_3_4         "Build Python 3.4 bindings" FALSE)
option(YT_BUILD_ENABLE_PERL               "Build Perl bindings" FALSE)
option(YT_BUILD_ENABLE_CPP_TESTS          "Build C++ integration tests" TRUE)
option(YT_BUILD_ENABLE_YP                 "Build YP" FALSE)
option(YT_BUILD_HAVE_RAGEL                "Do you have Ragel 6.8?" FALSE)
option(YT_BUILD_HAVE_BISON                "Do you have Bison 3.0?" FALSE)
option(YT_BUILD_HAVE_CYTHON               "Do you have Cython 2.5?" TRUE)
option(YT_BUILD_ENABLE_GDB_INDEX          "Build with .gdb_index section (speeds up gdb startup)" FALSE)
option(YT_USE_LTO                         "Enable link time optimizations" FALSE)
option(YT_USE_SSE                         "Enable using sse4 optimization flags" TRUE)
option(YT_USE_ASAN                        "Enable Address Sanitizer" FALSE)
option(YT_USE_TSAN                        "Enable Thread Sanitizer" FALSE)
option(YT_USE_MSAN                        "Enable Memory Sanitizer" FALSE)
option(YT_ASAN_USE_DSO_RUNTIME            "Use shared Address Sanitizer runtime" FALSE)
option(YT_ADD_HEADERS_TO_SRCS             "Add headers to YT_SRCS to let CLion analyze them" FALSE)

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

add_custom_target(compile-commands
  ln -sf "${CMAKE_BINARY_DIR}/compile_commands.json" "${CMAKE_SOURCE_DIR}/compile_commands.json"
  COMMENT "Linking compilation database from build dir to source tree..."
)

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

# Using CMake module FindLLVM.cmake from <root>/cmake directory.
find_package(LLVM)
link_directories(${LLVM_LIBRARY_DIRS})
include_directories(${LLVM_INCLUDE_DIRS})

# Sometimes we use Python for internal needs, so we need its interpreter.
find_package(PythonInterp)

# Discover Python world.
# COMPAT(sandello): Trigger building of all Python bindings.
set(_PYTHON_2_6_PATH "/usr/include/python2.6")
set(_PYTHON_2_7_PATH "/usr/include/python2.7")
set(_PYTHON_SKYNET_PATH "/skynet/python/include/python2.7")

if(DEFINED YT_BUILD_ENABLE_PYTHON)
  if(NOT DEFINED YT_BUILD_ENABLE_PYTHON_2_6 AND EXISTS ${_PYTHON_2_6_PATH})
    message(STATUS "[COMPAT] Setting YT_BUILD_ENABLE_PYTHON_2_6=ON due to YT_BUILD_ENABLE_PYTHON")
    set(YT_BUILD_ENABLE_PYTHON_2_6 TRUE)
  endif()
  if(NOT DEFINED YT_BUILD_ENABLE_PYTHON_2_7 AND EXISTS ${_PYTHON_2_7_PATH})
    message(STATUS "[COMPAT] Setting YT_BUILD_ENABLE_PYTHON_2_7=ON due to YT_BUILD_ENABLE_PYTHON")
    set(YT_BUILD_ENABLE_PYTHON_2_7 TRUE)
  endif()
  if(NOT DEFINED YT_BUILD_ENABLE_PYTHON_SKYNET AND EXISTS ${_PYTHON_SKYNET_PATH})
    message(STATUS "[COMPAT] Setting YT_BUILD_ENABLE_PYTHON_SKYNET=ON due to YT_BUILD_ENABLE_PYTHON")
    set(YT_BUILD_ENABLE_PYTHON_SKYNET TRUE)
  endif()
  message(STATUS "[COMPAT] Unsetting YT_BUILD_ENABLE_PYTHON")
  unset(YT_BUILD_ENABLE_PYTHON CACHE)
endif()

if(NOT DEFINED YT_BUILD_ENABLE_PYTHON)
  if(YT_BUILD_ENABLE_PYTHON_2_6 OR YT_BUILD_ENABLE_PYTHON_2_7 OR YT_BUILD_ENABLE_PYTHON_SKYNET)
    set(YT_BUILD_ENABLE_PYTHON TRUE)
    message(STATUS "[COMPAT] Setting YT_BUILD_ENABLE_PYTHON=ON")
  endif()
endif()

set(PYTHON_KINDS)
mark_as_advanced(PYTHON_KINDS)

function(CHECK_PYTHON_KIND kind include_dir)
  if(NOT EXISTS ${include_dir})
    message(FATAL_ERROR "[PYTHON] Kind \"${kind}\": Could not find include directory \"${include_dir}\": path does not exist")
  endif()
  message(STATUS "[PYTHON] Kind \"${kind}\": Using -I${include_dir}")
  set(PYTHON_${kind}_INCLUDE_DIR "${include_dir}" PARENT_SCOPE)
  mark_as_advanced(PYTHON_${kind}_INCLUDE_DIR)
  set(PYTHON_KINDS ${PYTHON_KINDS} ${kind} PARENT_SCOPE)
endfunction()

# Discover Python 2.6 world.
if(YT_BUILD_ENABLE_PYTHON_2_6)
  check_python_kind("2_6" "/usr/include/python2.6")
endif()

# Discover Python 2.7 world.
if(YT_BUILD_ENABLE_PYTHON_2_7)
  check_python_kind("2_7" "/usr/include/python2.7")
endif()

# Discover Python Skynet world.
if(YT_BUILD_ENABLE_PYTHON_SKYNET)
  check_python_kind("SKYNET" "/skynet/python/include/python2.7")
endif()

# Discover Python 3.4 world.
if(YT_BUILD_ENABLE_PYTHON_3_4)
  check_python_kind("3_4" "/usr/include/python3.4")
endif()

message(STATUS "[PYTHON] Using following kinds of Python: \"${PYTHON_KINDS}\"")

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

# Check for cython
if(YT_BUILD_HAVE_CYTHON)
  find_program(CYTHON_EXECUTABLE cython)
  mark_as_advanced(CYTHON_EXECUTABLE)
  if(NOT CYTHON_EXECUTABLE)
    message(FATAL_ERROR "Could not find cython")
  endif()
  message(STATUS "Found cython: ${CYTHON_EXECUTABLE}")
endif()

################################################################################
# Configure includes.

include_directories(
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
  ${CMAKE_SOURCE_DIR}/yt/contrib/tclap # header-only library
  ${CMAKE_SOURCE_DIR}/contrib/libs/sparsehash/src # header-only library
  ${CMAKE_SOURCE_DIR}/contrib/libs/re2 # re2 contains relative includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf # protobuf contains relative includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/protobuf/google/protobuf # protobuf contains relative includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/openssl/include # openssl contains relative includes
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc
  ${CMAKE_SOURCE_DIR}/contrib/libs/grpc/include
  ${CMAKE_SOURCE_DIR}/contrib/libs/brotli/include # brotli contains relative includes
)

################################################################################
# Assemble project.

add_subdirectory(cmake/deps-arcadia-contrib-libs-base64)
add_subdirectory(cmake/deps-arcadia-contrib-libs-brotli)
add_subdirectory(cmake/deps-arcadia-contrib-libs-c--ares)
add_subdirectory(cmake/deps-arcadia-contrib-libs-double--conversion)
add_subdirectory(cmake/deps-arcadia-contrib-libs-farmhash)
add_subdirectory(cmake/deps-arcadia-contrib-libs-gmock)
add_subdirectory(cmake/deps-arcadia-contrib-libs-grpc)
add_subdirectory(cmake/deps-arcadia-contrib-libs-gtest)
add_subdirectory(cmake/deps-arcadia-contrib-libs-libbz2)
add_subdirectory(cmake/deps-arcadia-contrib-libs-lz4)
add_subdirectory(cmake/deps-arcadia-contrib-libs-lzmasdk)
add_subdirectory(cmake/deps-arcadia-contrib-libs-minilzo)
add_subdirectory(cmake/deps-arcadia-contrib-libs-msgpack)
add_subdirectory(cmake/deps-arcadia-contrib-libs-nanopb)
add_subdirectory(cmake/deps-arcadia-contrib-libs-openssl)
add_subdirectory(cmake/deps-arcadia-contrib-libs-protobuf)
add_subdirectory(cmake/deps-arcadia-contrib-libs-re2)
add_subdirectory(cmake/deps-arcadia-contrib-libs-snappy)
add_subdirectory(cmake/deps-arcadia-contrib-libs-yajl)
add_subdirectory(cmake/deps-arcadia-contrib-libs-zlib)
add_subdirectory(cmake/deps-arcadia-contrib-libs-zstd)
add_subdirectory(cmake/deps-arcadia-library-colorizer)
add_subdirectory(cmake/deps-arcadia-library-getopt)
add_subdirectory(cmake/deps-arcadia-library-http)
add_subdirectory(cmake/deps-arcadia-library-lfalloc)
add_subdirectory(cmake/deps-arcadia-library-malloc-api)
add_subdirectory(cmake/deps-arcadia-library-openssl)
add_subdirectory(cmake/deps-arcadia-library-streams-brotli)
add_subdirectory(cmake/deps-arcadia-library-streams-lz)
add_subdirectory(cmake/deps-arcadia-library-streams-lzop)
add_subdirectory(cmake/deps-arcadia-library-string_utils-base64)
add_subdirectory(cmake/deps-arcadia-util)
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  add_subdirectory(cmake/deps-yt-contrib-coredumper)
endif()
add_subdirectory(cmake/deps-yt-contrib-jerasure)
add_subdirectory(cmake/deps-yt-contrib-fastlz)
add_subdirectory(cmake/deps-yt-contrib-quicklz)
add_subdirectory(cmake/deps-yt-contrib-http-parser)
if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  add_subdirectory(cmake/deps-yt-contrib-portoapi)
endif()
add_subdirectory(cmake/deps-yt-contrib-zstd-legacy)
add_subdirectory(cmake/deps-non_arcadia_contrib-benchmark)
add_subdirectory(python)
add_subdirectory(yt)
if (YT_BUILD_HAVE_CYTHON AND YT_BUILD_ENABLE_YP)
  add_subdirectory(yp)
endif()

if(YT_BUILD_ENABLE_PERL)
  add_subdirectory(perl)
endif()

################################################################################
# Packaging.

# Check if we can build package. We must build all bindings,
# because otherwise Debian scripts would produce empty packages.

set(YT_CAN_BUILD_PACKAGE TRUE)
if(NOT UNIX)
  set(YT_CAN_BUILD_PACKAGE FALSE)
  message(STATUS "Disabling packaging because of non-Unix environment")
endif()
if(NOT CMAKE_GENERATOR STREQUAL "Unix Makefiles")
  set(YT_CAN_BUILD_PACKAGE FALSE)
  message(STATUS "Disabling packaging because CMake generator is '${CMAKE_GENERATOR}' rather than 'Unix Makefiles'")
endif()

if(YT_CAN_BUILD_PACKAGE)
  add_subdirectory(debian)

  execute_process(COMMAND lsb_release --codename --short OUTPUT_VARIABLE LSB_RELEASE)
  if(${LSB_RELEASE} MATCHES "trusty")
    set(DCH_VENDOR_FLAG "--vendor")
  else()
    set(DCH_VENDOR_FLAG "--distributor")
  endif()

  add_custom_target(changelog touch changelog-stamp
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

  add_custom_target(proto-changelog
    COMMAND rm -f ${CMAKE_SOURCE_DIR}/python/yandex-yt-python-proto/debian/changelog
    COMMAND dch
      --create
      ${DCH_VENDOR_FLAG} "yandex"
      --package "yandex-yt-python-proto"
      --distribution "unstable"
      --newversion "${YT_RPC_PROXY_PROTOCOL_VERSION}"
      --urgency "low"
      --force-distribution
      "Proto package release."
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/python/yandex-yt-python-proto
    COMMENT "Creating yandex-yt-python-proto changelog..."
  )

  add_custom_target(driver-rpc-changelog
    COMMAND rm -f ${CMAKE_SOURCE_DIR}/python/yandex-yt-python-driver-rpc/debian/changelog
    COMMAND dch
      --create
      ${DCH_VENDOR_FLAG} "yandex"
      --package "yandex-yt-python-driver-rpc"
      --distribution "unstable"
      --newversion "${YT_RPC_PYTHON_BINDINGS_VERSION}"
      --urgency "low"
      --force-distribution
      "Proto package release."
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/python/yandex-yt-python-driver-rpc
    COMMENT "Creating yandex-yt-python-driver-rpc changelog..."
  )

  find_package(Git)
  if (NOT GIT_FOUND)
    message(FATAL_ERROR "Git is required for packaging")
  endif()

  add_custom_target(srcs
      COMMAND ${CMAKE_SOURCE_DIR}/debian/prepare_sources.py ${GIT_EXECUTABLE} ${CMAKE_SOURCE_DIR}
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  )

  install(
    DIRECTORY ${CMAKE_BINARY_DIR}/src
    DESTINATION share/yandex-yt/${YT_VERSION}
    COMPONENT sources
  )

  add_custom_target(package touch package-stamp
    COMMAND debuild
      -e SOURCE_DIR=${CMAKE_SOURCE_DIR}
      --no-tgz-check
      --no-lintian
      --check-dirname-level 0
      -b
    COMMAND mv
      ../yandex-*${YT_VERSION}*
      ${CMAKE_BINARY_DIR}/ARTIFACTS
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
    COMMENT "Packaging YT..."
    DEPENDS srcs changelog
  )

  add_custom_target(python-package touch python-package-stamp
    COMMAND
      cat ${CMAKE_BINARY_DIR}/debian/changelog
      | sed 's/^yandex-yt/yandex-yt-python-driver/'
      > ${CMAKE_SOURCE_DIR}/python/yandex-yt-python-driver/debian/changelog
    COMMAND ${CMAKE_SOURCE_DIR}/python/build_driver.sh
    COMMAND ${CMAKE_SOURCE_DIR}/python/build_proto.sh
    COMMAND mv
      ../yandex-*${YT_VERSION}*
      ${CMAKE_BINARY_DIR}/ARTIFACTS
    COMMAND mv
      ../yandex-yt-python-*
      ${CMAKE_BINARY_DIR}/ARTIFACTS
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/python
    COMMENT "Packaging YT Python..."
    DEPENDS changelog proto-changelog driver-rpc-changelog
  )

  function(HOOK_FOR_DEBIAN package component)
    add_custom_target(
      install-debian-${package}
      ${CMAKE_COMMAND}
        -DCOMPONENT=${component}
        -P ${CMAKE_BINARY_DIR}/cmake_install.cmake
      COMMENT "Installing for package \"${package}\"..."
    )
  endfunction()

  # Check-list for adding a new package:
  # 1) Here, below, in Debian-CMake hook.
  # 2) debian/control
  # 3) debian/rules
  hook_for_debian(yandex-yt all_binaries)
  hook_for_debian(yandex-yt-master cell_master)
  hook_for_debian(yandex-yt-scheduler scheduler)
  hook_for_debian(yandex-yt-controller-agent controller_agent)
  hook_for_debian(yandex-yt-node cell_node)
  hook_for_debian(yandex-yt-proxy cell_proxy)
  hook_for_debian(yandex-yt-src sources)
  hook_for_debian(yandex-yt-standalone standalone)
  hook_for_debian(yandex-yt-http-proxy nodejs)
  hook_for_debian(yandex-yt-perl perl-module)
  hook_for_debian(yandex-yt-perl-${YT_ABI_VERSION} perl-abi)
  foreach(PYTHON_KIND IN LISTS PYTHON_KINDS)
    string(TOLOWER ${PYTHON_KIND} PYTHON_KIND_LOWER)
    string(REPLACE "_" "-" PYTHON_KIND_DASHED ${PYTHON_KIND_LOWER})
    hook_for_debian(yandex-yt-python-${PYTHON_KIND_DASHED}-yson yt-python-${PYTHON_KIND_DASHED}-yson)
    hook_for_debian(yandex-yt-python-${PYTHON_KIND_DASHED}-driver yt-python-${PYTHON_KIND_DASHED}-driver)
  endforeach()
  hook_for_debian(yandex-yp yp)
else()
  add_custom_target(package touch package-stamp
    COMMAND ${CMAKE_COMMAND} -E echo -- PACKAGE BUILD IS DISABLED -- SEE CMAKE OUTPUT && exit 100
  )
  add_custom_target(python-package touch python-package-stamp
    COMMAND ${CMAKE_COMMAND} -E echo -- PACKAGE BUILD IS DISABLED -- SEE CMAKE OUTPUT && exit 100
  )
endif()

add_custom_target(version
  touch version-stamp
  COMMAND echo "${YT_VERSION}" > ytversion
  WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
  COMMENT "Stamping YT..."
)
