set(NAME deps-yt-contrib-minilzo)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/minilzo)
set(SRCS
  ${BASE}/minilzo.c
  ${BASE}/minilzo_crc.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_library(${NAME} STATIC ${SRCS})

