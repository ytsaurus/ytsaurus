################################################################################
IF(YMAKE)
################################################################################

LIBRARY()

OWNER(sandello)

SET(YT_VERSION_MAJOR 18)
SET(YT_VERSION_MINOR 0)
SET(YT_VERSION_PATCH 0)
SET(YT_VERSION "18.0.0-arcadia")

SET(YT_BUILD_HOST "")
SET(YT_BUILD_MACHINE "")
SET(YT_BUILD_TIME "")

SET(HAVE_CXXABI_H 0)
SET(HAVE_DLFCN_H 1)
SET(HAVE_EXECINFO_H 1)
SET(HAVE_LIBUNWIND_H 0)
SET(HAVE_PTHREAD_H 1)
SET(HAVE_SYS_TYPES_H 1)
SET(HAVE_SYS_UCONTEXT_H 1)
SET(HAVE_UCONTEXT_H 1)
SET(HAVE_UNISTD_H 1)
SET(HAVE_UNWIND_H 0)
SET(PC_FROM_UCONTEXT "uc_mcontext.gregs[REG_RIP]")

SRCS(
  config.h.in
  build.cpp.in
  build.h
)

END()

################################################################################
ELSE()
################################################################################

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/config.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/config.h
  @ONLY
)

configure_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/build.cpp.in
  ${CMAKE_CURRENT_BINARY_DIR}/build.cpp
  @ONLY
)

add_library(yt-build ${CMAKE_CURRENT_BINARY_DIR}/build.cpp)

################################################################################
ENDIF()
################################################################################
