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

if (YT_BUILD_DEB_PACKAGE)
  set(CPACK_GENERATOR "DEB")
  set(CPACK_PACKAGE_DESCRIPTION
    "A scalable distributed storage and computation platform.")
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY
    "http://wiki.yandex-team.ru/YT/")
  set(CPACK_PACKAGE_NAME "yandex-yt")
  set(CPACK_PACKAGE_VENDOR "Yandex")
  set(CPACK_PACKAGE_CONTACT "Ivan Pouzyrevsky <sandello@yandex-team.ru>")
  set(CPACK_PACKAGE_VERSION "0.1.0")
  set(CPACK_PACKAGE_VERSION_MAJOR "0")
  set(CPACK_PACKAGE_VERSION_MINOR "1")
  set(CPACK_PACKAGE_VERSION_PATCH "0")
  set(CPACK_DEBIAN_PACKAGE_DEPENDS "libc6 (>= 2.4), libgcc1 (>= 1:4.1.1)")
  set(CPACK_DEBIAN_PACKAGE_SECTION "unknown")
  set(CPACK_DEBIAN_PACKAGE_PRIORITY "extra")
  set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE "amd64")
  set(CPACK_STRIP_FILES "")
  set(CPACK_SYSTEM_NAME "amd64")
  set(CPACK_TOPLEVEL_TAG "amd64")

  include(CPack)
endif()
