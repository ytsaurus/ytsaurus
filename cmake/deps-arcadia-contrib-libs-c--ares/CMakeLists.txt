set(NAME deps-arcadia-contrib-libs-c--ares)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/c-ares)
set(C_SRCS
  ${BASE}/ares__close_sockets.c
  ${BASE}/ares__get_hostent.c
  ${BASE}/ares__read_line.c
  ${BASE}/ares__timeval.c
  ${BASE}/ares_cancel.c
  ${BASE}/ares_create_query.c
  ${BASE}/ares_data.c
  ${BASE}/ares_destroy.c
  ${BASE}/ares_expand_name.c
  ${BASE}/ares_expand_string.c
  ${BASE}/ares_fds.c
  ${BASE}/ares_free_hostent.c
  ${BASE}/ares_free_string.c
  ${BASE}/ares_getenv.c
  ${BASE}/ares_gethostbyaddr.c
  ${BASE}/ares_gethostbyname.c
  ${BASE}/ares_getnameinfo.c
  ${BASE}/ares_getopt.c
  ${BASE}/ares_getsock.c
  ${BASE}/ares_init.c
  ${BASE}/ares_library_init.c
  ${BASE}/ares_llist.c
  ${BASE}/ares_mkquery.c
  ${BASE}/ares_nowarn.c
  ${BASE}/ares_options.c
  ${BASE}/ares_parse_a_reply.c
  ${BASE}/ares_parse_aaaa_reply.c
  ${BASE}/ares_parse_mx_reply.c
  ${BASE}/ares_parse_naptr_reply.c
  ${BASE}/ares_parse_ns_reply.c
  ${BASE}/ares_parse_ptr_reply.c
  ${BASE}/ares_parse_soa_reply.c
  ${BASE}/ares_parse_srv_reply.c
  ${BASE}/ares_parse_txt_reply.c
  ${BASE}/ares_platform.c
  ${BASE}/ares_process.c
  ${BASE}/ares_query.c
  ${BASE}/ares_search.c
  ${BASE}/ares_send.c
  ${BASE}/ares_strcasecmp.c
  ${BASE}/ares_strdup.c
  ${BASE}/ares_strerror.c
  ${BASE}/ares_timeout.c
  ${BASE}/ares_version.c
  ${BASE}/ares_writev.c
  ${BASE}/bitncmp.c
  ${BASE}/inet_net_pton.c
  ${BASE}/inet_ntop.c
)

add_definitions(
  -DCARES_STATICLIB
  -DHAVE_CONFIG_H
)

set_source_files_properties(${C_SRCS} PROPERTIES LANGUAGE C)

include_directories(${BASE})

add_library(${NAME} STATIC ${C_SRCS} ${BASE}/atomic.cpp)

if (_is_gcc)
  set_target_properties(${NAME} PROPERTIES COMPILE_FLAGS -Wno-error)
endif()
