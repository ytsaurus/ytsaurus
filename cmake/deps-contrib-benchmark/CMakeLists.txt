set(BASE ${CMAKE_SOURCE_DIR}/contrib/benchmark/src)
set(SRCS
  ${BASE}/benchmark.cc
  ${BASE}/colorprint.cc
  ${BASE}/commandlineflags.cc
  ${BASE}/sleep.cc
  ${BASE}/sysinfo.cc
  ${BASE}/walltime.cc
  ${BASE}/re_posix.cc
)

check_include_files(regex HAVE_STD_REGEX)
check_include_files(gnuregex.h HAVE_GNU_POSIX_REGEX)
check_include_files(regex.h HAVE_POSIX_REGEX)

# Set regular expression engine.
if(HAVE_STD_REGEX)
  set(SRCS ${SRCS} ${BASE}/re_std.cc)
  add_definitions(-DHAVE_STD_REGEX)
elseif(HAVE_GNU_POSIX_REGEX)
  set(SRCS ${SRCS} ${BASE}/re_posix.cc)
  add_definitions(-DHAVE_GNU_POSIX_REGEX)
elseif(HAVE_POSIX_REGEX)
  set(SRCS ${SRCS} ${BASE}/re_posix.cc)
  add_definitions(-DHAVE_POSIX_REGEX)
endif()

# Set OS
if(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  add_definitions(-DOS_LINUX)
endif()

if(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  add_definitions(-DOS_WINDOWS)
endif()

# Set CPU
if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "x86")
  add_definitions(-DARCH_X86)
endif()

# Add a debug definition so we can make decisions in the compilation.
if(NOT WIN32)
  set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-strict-aliasing")
endif()

# Configure includes.
include_directories(${BASE}/../include)
include_directories(${BASE}/../src)

add_library(yt-contrib-benchmark STATIC ${SRCS})

