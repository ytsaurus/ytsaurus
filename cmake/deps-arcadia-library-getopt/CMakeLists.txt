set( BASE ${CMAKE_SOURCE_DIR}/library/getopt )
set( SRCS
  ${BASE}/common/last_getopt.cpp
  ${BASE}/common/last_getopt_opt.cpp
  ${BASE}/common/last_getopt_opts.cpp
  ${BASE}/common/last_getopt_parser.cpp
  ${BASE}/common/last_getopt_parse_result.cpp
  ${BASE}/common/modchooser.cpp
  ${BASE}/print.cpp
)

include_directories(
  ${BASE}
  ${CMAKE_BINARY_DIR}
  ${CMAKE_SOURCE_DIR}
)

add_library( ytext-arcadia-library-getopt STATIC ${SRCS} )

target_link_libraries( ytext-arcadia-library-getopt
  ytext-arcadia-util
)

