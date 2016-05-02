set(NAME deps-arcadia-contrib-libs-minilzo)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/minilzo)
set(SRCS
  ${BASE}/minilzo.c
  ${BASE}/lzo_crc.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

