if (NOT YT_BUILD_BRANCH)
  set(YT_BUILD_BRANCH "local")
endif()

if (NOT YT_BUILD_NUMBER)
  set(YT_BUILD_NUMBER 0)
endif()

find_program(_GIT NAMES git)

if (_GIT AND EXISTS ${PROJECT_SOURCE_DIR}/.git AND IS_DIRECTORY ${PROJECT_SOURCE_DIR}/.git)
    # This is a VERY dirty hack to make make re-run cmake (pun intended)
    # when a different commit is checked out.
    configure_file(
      ${PROJECT_SOURCE_DIR}/.git/logs/HEAD
      ${CMAKE_CURRENT_BINARY_DIR}/git_logs_HEAD
      COPYONLY)

    set(_GIT ${_GIT} CACHE INTERNAL "")
endif()

if (NOT YT_BUILD_GIT_DEPTH)
  set(YT_BUILD_GIT_DEPTH 0)
  if (_GIT)
    find_program(_PYTHON NAMES python)
    if (_PYTHON)
      execute_process(
        COMMAND ${_PYTHON} git-depth.py
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
        OUTPUT_VARIABLE YT_BUILD_GIT_DEPTH
        ERROR_VARIABLE GIT_ERROR
        OUTPUT_STRIP_TRAILING_WHITESPACE)
      if(GIT_ERROR)
        message(WARNING
          "Error running command `python git-depth.py` in source directory \
          for getting local commit depth.\nStderr:\n${GIT_ERROR}")
        set(YT_BUILD_GIT_DEPTH 0)
      endif()
    endif()
  endif()
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(YT_BUILD_TYPE "debug")
endif()
if(YT_USE_ASAN)
  set(YT_BUILD_TYPE "asan")
endif()
if(YT_USE_TSAN)
  set(YT_BUILD_TYPE "tsan")
endif()
if(YT_USE_MSAN)
  set(YT_BUILD_TYPE "msan")
endif()
if(_is_clang)
  set(YT_BUILD_TYPE "clang")
endif()

# Set the YT build version
set(YT_VERSION_MAJOR 19)
set(YT_VERSION_MINOR 3)
set(YT_VERSION_PATCH ${YT_BUILD_GIT_DEPTH})
set(YT_ABI_VERSION "${YT_VERSION_MAJOR}.${YT_VERSION_MINOR}")

# Set the YT RPC protocol version
include(${PROJECT_SOURCE_DIR}/yt/RpcProxyProtocolVersion.txt)

# Clone some version attributes from YT to YP
set(YP_BUILD_BRANCH "${YT_BUILD_BRANCH}")
set(YP_BUILD_NUMBER "${YT_BUILD_NUMBER}")
set(YP_BUILD_GIT_DEPTH "${YT_BUILD_GIT_DEPTH}")
set(YP_BUILD_TYPE "${YT_BUILD_TYPE}")

# Set the YP build version
set(YP_VERSION_MAJOR 0)
set(YP_VERSION_MINOR 2)
set(YP_VERSION_PATCH ${YP_BUILD_GIT_DEPTH})

# Get the build name, hostname and user name
find_program(_HOSTNAME NAMES hostname)
find_program(_UNAME NAMES uname)
find_program(_DATE NAMES date)
find_program(_WHOAMI NAMES whoami)

if(_HOSTNAME)
  set(_HOSTNAME ${_HOSTNAME} CACHE INTERNAL "")
  execute_process(
    COMMAND ${_HOSTNAME}
    OUTPUT_VARIABLE YT_BUILD_HOST
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  string(REGEX REPLACE "[/\\\\+<> #]" "-" YT_BUILD_HOST "${YT_BUILD_HOST}")
else()
  set(YT_BUILD_HOST "unknown")
endif()

if(_UNAME)
  set(_UNAME ${_UNAME} CACHE INTERNAL "")
  execute_process(
    COMMAND ${_UNAME} -a
    OUTPUT_VARIABLE YT_BUILD_MACHINE
    OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
  set(YT_BUILD_MACHINE "unknown")
endif()

if(_DATE)
  set(_DATE ${_DATE} CACHE INTERNAL "")
  execute_process(
    COMMAND ${_DATE}
    OUTPUT_VARIABLE YT_BUILD_TIME
    OUTPUT_STRIP_TRAILING_WHITESPACE)
else()
  set(YT_BUILD_TIME "unknown")
endif()

# Teamcity will pass empty string as YT_BUILD_USERNAME to
# suppress meaningless teamcity user mention in version.
if (NOT DEFINED YT_BUILD_USERNAME)
  if(_WHOAMI)
    set(_WHOAMI ${_WHOAMI} CACHE INTERNAL "")
    execute_process(
      COMMAND ${_WHOAMI}
      OUTPUT_VARIABLE YT_BUILD_USERNAME
      OUTPUT_STRIP_TRAILING_WHITESPACE)
  else()
    set(YT_BUILD_USERNAME "")
  endif()
endif()

if (NOT YT_BUILD_VCS_NUMBER)
  if (_GIT AND EXISTS ${PROJECT_SOURCE_DIR}/.git)
    execute_process(
      COMMAND ${_GIT} -C ${PROJECT_SOURCE_DIR} rev-parse HEAD
      OUTPUT_VARIABLE YT_BUILD_VCS_NUMBER
      ERROR_VARIABLE GIT_ERROR
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    if(GIT_ERROR)
      message(WARNING
        "Error running command `${_GIT} -C ${PROJECT_SOURCE_DIR} rev-parse HEAD` \
        for getting local build commit SHA.\nStderr:\n${GIT_ERROR}")
      set(YT_BUILD_VCS_NUMBER "0000000000")
    else()
      string(SUBSTRING ${YT_BUILD_VCS_NUMBER} 0 10 YT_BUILD_VCS_NUMBER)
      message(STATUS "Setting local build commit SHA `${YT_BUILD_VCS_NUMBER}`")
    endif()
  else()
    set(YT_BUILD_VCS_NUMBER "0000000000")
  endif()
endif()

# Clone some version attributes from YT to YP
set(YP_BUILD_HOST "${YT_BUILD_HOST}")
set(YP_BUILD_MACHINE "${YT_BUILD_MACHINE}")
set(YP_BUILD_TIME "${YT_BUILD_TIME}")
set(YP_BUILD_USERNAME "${YT_BUILD_USERNAME}")
set(YP_BUILD_VCS_NUMBER "${YT_BUILD_VCS_NUMBER}")

# Construct the full YT version
set(YT_VERSION "${YT_VERSION_MAJOR}.${YT_VERSION_MINOR}.${YT_VERSION_PATCH}")
set(YT_VERSION "${YT_VERSION}-${YT_BUILD_BRANCH}")
if(YT_BUILD_TYPE)
  set(YT_VERSION "${YT_VERSION}-${YT_BUILD_TYPE}")
endif()
if(YT_BUILD_VCS_NUMBER)
  set(YT_VERSION "${YT_VERSION}~${YT_BUILD_VCS_NUMBER}")
endif()
if(YT_BUILD_USERNAME)
  set(YT_VERSION "${YT_VERSION}+${YT_BUILD_USERNAME}")
endif()

# Construct the full YP version
set(YP_VERSION "${YP_VERSION_MAJOR}.${YP_VERSION_MINOR}.${YP_VERSION_PATCH}")
set(YP_VERSION "${YP_VERSION}-${YP_BUILD_BRANCH}")
if(YP_BUILD_TYPE)
  set(YP_VERSION "${YP_VERSION}-${YP_BUILD_TYPE}")
endif()
set(YP_VERSION "${YP_VERSION}~${YP_BUILD_VCS_NUMBER}")
if(YP_BUILD_USERNAME)
  set(YP_VERSION "${YP_VERSION}+${YP_BUILD_USERNAME}")
endif()

# Underscore is forbidden in the version
string(REPLACE "_" "-" YT_VERSION ${YT_VERSION})
string(REPLACE "_" "-" YP_VERSION ${YP_VERSION})
# Slash is forbidden in the version
string(REPLACE "/" "-" YT_VERSION ${YT_VERSION})
string(REPLACE "/" "-" YP_VERSION ${YP_VERSION})
# Dot is forbidden in proper keywords
string(REPLACE "." "_" YT_ABI_VERSION_U ${YT_ABI_VERSION})
