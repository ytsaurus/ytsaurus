# Set the build version
set(YT_VERSION_MAJOR 0)
set(YT_VERSION_MINOR 16)
set(YT_VERSION_PATCH 6)

if (NOT YT_BUILD_BRANCH)
  set(YT_BUILD_BRANCH "unknown")
endif()

if (NOT YT_BUILD_NUMBER)
  set(YT_BUILD_NUMBER 0)
endif()

if (NOT YT_BUILD_VCS_NUMBER)
  set(YT_BUILD_VCS_NUMBER "local")
endif()

set(YT_VERSION "${YT_VERSION_MAJOR}.${YT_VERSION_MINOR}.${YT_VERSION_PATCH}")
set(YT_VERSION "${YT_VERSION}-${YT_BUILD_BRANCH}")
set(YT_VERSION "${YT_VERSION}~${YT_BUILD_NUMBER}")
set(YT_VERSION "${YT_VERSION}+${YT_BUILD_VCS_NUMBER}")
# underscore is forbidden in the version
STRING(REGEX REPLACE "_" "-" YT_VERSION ${YT_VERSION})

# Get the build name and hostname
find_program(_HOSTNAME NAMES hostname)
find_program(_UNAME NAMES uname)
find_program(_DATE NAMES date)

if (_HOSTNAME)
  set(_HOSTNAME ${_HOSTNAME} CACHE INTERNAL "")
  execute_process(
    COMMAND ${_HOSTNAME}
    OUTPUT_VARIABLE YT_BUILD_HOST
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  string(REGEX REPLACE "[/\\\\+<> #]" "-" YT_BUILD_HOST "${YT_BUILD_HOST}")
else()
  set(YT_BUILD_HOST "unknown")
endif()

if (_UNAME)
  set(_UNAME ${_UNAME} CACHE INTERNAL "")
  execute_process(
    COMMAND ${_UNAME} -a
    OUTPUT_VARIABLE YT_BUILD_MACHINE
    OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
  set(YT_BUILD_MACHINE "unknown")
endif()

if (_DATE)
  set(_DATE ${_DATE} CACHE INTERNAL "")
  execute_process(
    COMMAND ${_DATE}
    OUTPUT_VARIABLE YT_BUILD_TIME
    OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
  set(YT_BUILD_TIME "unknown")
endif()

