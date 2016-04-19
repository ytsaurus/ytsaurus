set(NAME deps-yt-contrib-zstd)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/zstd)
set(SRCS
  ${BASE}/lib/fse.c
  ${BASE}/lib/huff0.c
  ${BASE}/lib/zstd_compress.c
  ${BASE}/lib/zstd_decompress.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

include_directories(${BASE}/lib ${BASE}/lib/legacy)

add_library(${NAME} STATIC ${SRCS})

