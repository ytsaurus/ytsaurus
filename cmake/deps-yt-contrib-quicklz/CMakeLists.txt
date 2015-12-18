set(NAME deps-yt-contrib-quicklz)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/quicklz)
set(SRCS
  ${BASE}/quicklz.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

