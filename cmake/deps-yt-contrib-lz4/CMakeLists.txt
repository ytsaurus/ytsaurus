set(NAME deps-yt-contrib-lz4)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/lz4)
set(SRCS
  ${BASE}/lz4.c
  ${BASE}/lz4hc.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

