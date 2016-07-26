set(NAME deps-yt-contrib-zstd)
set(BASE ${CMAKE_SOURCE_DIR}/yt/contrib/zstd)
set(SRCS
  ${BASE}/lib/common/fse_decompress.c
  ${BASE}/lib/common/zstd_common.c
  ${BASE}/lib/common/entropy_common.c
  ${BASE}/lib/common/xxhash.c
  ${BASE}/lib/compress/fse_compress.c
  ${BASE}/lib/compress/huf_compress.c
  ${BASE}/lib/compress/zbuff_compress.c
  ${BASE}/lib/compress/zstd_compress.c
  ${BASE}/lib/decompress/huf_decompress.c
  ${BASE}/lib/decompress/zbuff_decompress.c
  ${BASE}/lib/decompress/zstd_decompress.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

include_directories(${BASE}/lib ${BASE}/lib/legacy ${BASE}/lib/common)

add_library(${NAME} STATIC ${SRCS})

