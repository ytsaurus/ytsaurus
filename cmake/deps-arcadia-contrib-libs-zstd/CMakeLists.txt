set(NAME deps-arcadia-contrib-libs-zstd)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/zstd)
set(SRCS
  ${BASE}/common/entropy_common.c
  ${BASE}/common/error_private.c
  ${BASE}/common/fse_decompress.c
  ${BASE}/common/xxhash.c
  ${BASE}/common/zstd_common.c
  ${BASE}/compress/fse_compress.c
  ${BASE}/compress/huf_compress.c
  ${BASE}/compress/zstd_compress.c
  ${BASE}/decompress/huf_decompress.c
  ${BASE}/decompress/zstd_decompress.c
  ${BASE}/dictBuilder/divsufsort.c
  ${BASE}/dictBuilder/zdict.c
  ${BASE}/legacy/zstd_v01.c
  ${BASE}/legacy/zstd_v02.c
  ${BASE}/legacy/zstd_v03.c
  ${BASE}/legacy/zstd_v04.c
  ${BASE}/legacy/zstd_v05.c
  ${BASE}/legacy/zstd_v06.c
  ${BASE}/legacy/zstd_v07.c
)

set_source_files_properties(${SRCS} PROPERTIES LANGUAGE C)

add_definitions(-DZSTD_LEGACY_SUPPORT=1)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${BASE}
  ${BASE}/common
  ${BASE}/legacy
)

add_library(${NAME} STATIC ${SRCS})
