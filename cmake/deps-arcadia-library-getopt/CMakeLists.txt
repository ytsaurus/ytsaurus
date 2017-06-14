set(NAME deps-arcadia-library-getopt)
set(BASE ${CMAKE_SOURCE_DIR}/library/getopt)
set(SRCS
  ${BASE}/small/last_getopt.cpp
  ${BASE}/small/last_getopt_easy_setup.cpp
  ${BASE}/small/last_getopt_opt.cpp
  ${BASE}/small/last_getopt_opts.cpp
  ${BASE}/small/last_getopt_parser.cpp
  ${BASE}/small/last_getopt_parse_result.cpp
  ${BASE}/small/modchooser.cpp
  ${BASE}/small/posix_getopt.cpp
  ${BASE}/small/ygetopt.cpp
  ${BASE}/print.cpp
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

target_link_libraries(${NAME}
  deps-arcadia-util
  deps-arcadia-library-colorizer
)

