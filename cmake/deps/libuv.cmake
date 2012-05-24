if (UNIX)
  set( LTYPE SHARED )
  execute_process(
    COMMAND uname -s
    OUTPUT_VARIABLE STYPE
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
endif()

if (WIN32)
  set( LTYPE STATIC )
  set( STYPE "mingw" )
endif()

set( BASE ${CMAKE_SOURCE_DIR}/contrib/libuv )

set( SRCS_ARES
  ${BASE}/src/ares/ares__close_sockets.c
  ${BASE}/src/ares/ares__get_hostent.c
  ${BASE}/src/ares/ares__read_line.c
  ${BASE}/src/ares/ares__timeval.c
  ${BASE}/src/ares/ares_cancel.c
  ${BASE}/src/ares/ares_data.c
  ${BASE}/src/ares/ares_destroy.c
  ${BASE}/src/ares/ares_expand_name.c
  ${BASE}/src/ares/ares_expand_string.c
  ${BASE}/src/ares/ares_fds.c
  ${BASE}/src/ares/ares_free_hostent.c
  ${BASE}/src/ares/ares_free_string.c
  ${BASE}/src/ares/ares_gethostbyaddr.c
  ${BASE}/src/ares/ares_gethostbyname.c
  ${BASE}/src/ares/ares_getnameinfo.c
  ${BASE}/src/ares/ares_getopt.c
  ${BASE}/src/ares/ares_getsock.c
  ${BASE}/src/ares/ares_init.c
  ${BASE}/src/ares/ares_library_init.c
  ${BASE}/src/ares/ares_llist.c
  ${BASE}/src/ares/ares_mkquery.c
  ${BASE}/src/ares/ares_nowarn.c
  ${BASE}/src/ares/ares_options.c
  ${BASE}/src/ares/ares_parse_a_reply.c
  ${BASE}/src/ares/ares_parse_aaaa_reply.c
  ${BASE}/src/ares/ares_parse_mx_reply.c
  ${BASE}/src/ares/ares_parse_ns_reply.c
  ${BASE}/src/ares/ares_parse_ptr_reply.c
  ${BASE}/src/ares/ares_parse_srv_reply.c
  ${BASE}/src/ares/ares_parse_txt_reply.c
  ${BASE}/src/ares/ares_process.c
  ${BASE}/src/ares/ares_query.c
  ${BASE}/src/ares/ares_search.c
  ${BASE}/src/ares/ares_send.c
  ${BASE}/src/ares/ares_strcasecmp.c
  ${BASE}/src/ares/ares_strdup.c
  ${BASE}/src/ares/ares_strerror.c
  ${BASE}/src/ares/ares_timeout.c
  ${BASE}/src/ares/ares_version.c
  ${BASE}/src/ares/ares_writev.c
  ${BASE}/src/ares/bitncmp.c
  ${BASE}/src/ares/inet_net_pton.c
  ${BASE}/src/ares/inet_ntop.c
)

set( SRCS_UNIX
  ${BASE}/src/unix/core.c
  ${BASE}/src/unix/dl.c
  ${BASE}/src/unix/fs.c
  ${BASE}/src/unix/cares.c
  ${BASE}/src/unix/udp.c
  ${BASE}/src/unix/error.c
  ${BASE}/src/unix/process.c
  ${BASE}/src/unix/tcp.c
  ${BASE}/src/unix/pipe.c
  ${BASE}/src/unix/tty.c
  ${BASE}/src/unix/stream.c
)

# TODO(sandello): Properly integrate with .mk files in libuv.
message( STATUS "STYPE=${STYPE}") 
if( STYPE STREQUAL "SunOS" )
elseif( STYPE STREQUAL "Darwin" )
  set( _EV_CONFIG "config_darwin.h" )
  set( _EIO_CONFIG "config_darwin.h" )
  set( SRCS_PLATFORM ${SRCS_UNIX} ${BASE}/src/unix/darwin.c ${BASE}/src/unix/kqueue.c )
elseif( STYPE STREQUAL "Linux" )
  set( _EV_CONFIG "config_linux.h" )
  set( _EIO_CONFIG "config_linux.h" )
  set( SRCS_PLATFORM ${SRCS_UNIX} ${BASE}/src/unix/linux.c )
  include_directories( ${BASE}/src/ares/config_linux )
elseif( STYPE STREQUAL "mingw" )
  set( SRCS_PLATFORM
    ${BASE}/src/ares/windows_port.c
    ${BASE}/src/ares/ares_platform.c
    ${BASE}/src/win/threadpool.c
    ${BASE}/src/win/tcp.c
    ${BASE}/src/win/cares.c
    ${BASE}/src/win/udp.c
    ${BASE}/src/win/util.c
    ${BASE}/src/win/async.c
    ${BASE}/src/win/winapi.h
    ${BASE}/src/win/tty.c
    ${BASE}/src/win/stream.c
    ${BASE}/src/win/handle.c
    ${BASE}/src/win/req.c
    ${BASE}/src/win/pipe.c
    ${BASE}/src/win/process.c
    ${BASE}/src/win/loop-watcher.c
    ${BASE}/src/win/error.c
    ${BASE}/src/win/dl.c
    ${BASE}/src/win/winsock.c
    ${BASE}/src/win/winapi.c
    ${BASE}/src/win/core.c
    ${BASE}/src/win/getaddrinfo.c
    ${BASE}/src/win/timer.c
    ${BASE}/src/win/threads.c
    ${BASE}/src/win/fs-event.c
    ${BASE}/src/win/fs.c
  )
  include_directories( ${BASE}/src/ares/config_win32 )
endif()
  

add_library( ytext-libuv ${LTYPE}
  ${SRCS_ARES} ${SRCS_PLATFORM}
)

include_directories(
  ${BASE}/include
  ${BASE}/include/uv-private
  ${BASE}/src
)

#if (YT_BUILD_WITH_STLPORT)
#  target_link_libraries( ytext-libuv stlport )
#  if (CMAKE_COMPILER_IS_GNUCXX)
#    set_target_properties( ytext-libuv PROPERTIES LINK_FLAGS "-nodefaultlibs" )
#  endif()
#endif()

set_target_properties( ytext-libuv PROPERTIES
  VERSION   0.6.18
  SOVERSION 0.6
)

if (UNIX)
  set_target_properties( ytext-libuv PROPERTIES
    COMPILE_FLAGS "-pedantic -Wall -Wextra -Wno-unused-parameter"
    COMPILE_DEFINITIONS
    "_LARGEFILE_SOURCE;_FILE_OFFSET_BITS=64;EV_CONFIG_H=${_EV_CONFIG};EIO_CONFIG_H=${_EIO_CONFIG};EIO_STACKSIZE=262144;_GNU_SOURCE;HAVE_CONFIG_H"
  )
  target_link_libraries( ytext-libuv -lm -lrt )
endif()

if (WIN32)
  set_target_properties( ytext-libuv PROPERTIES
    COMPILE_DEFINITIONS
    "_WIN32_WINNT=0x0501;HAVE_CONFIG_H"
  )
endif()


install(
  TARGETS
  ytext-libuv
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib
)
