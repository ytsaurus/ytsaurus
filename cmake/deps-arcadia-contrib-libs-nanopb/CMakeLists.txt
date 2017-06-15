set(NAME deps-arcadia-contrib-libs-nanopb)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/nanopb)
set(SRCS
  ${BASE}/pb_common.c
  ${BASE}/pb_encode.c
  ${BASE}/pb_decode.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

