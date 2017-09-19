set(NAME deps-yt-contrib-http-parser)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/http-parser)
set(SRCS ${BASE}/http_parser.c)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})
