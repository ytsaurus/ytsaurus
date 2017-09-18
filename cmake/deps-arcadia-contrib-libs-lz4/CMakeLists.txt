set(NAME deps-arcadia-contrib-libs-lz4)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/lz4)
set(SRCS
  ${BASE}/lz4.c
  ${BASE}/lz4frame.c
  ${BASE}/lz4hc.c
  ${BASE}/xxhash.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_definitions(-DXXH_NAMESPACE=LZ4_)

add_library(${NAME} STATIC ${SRCS})
