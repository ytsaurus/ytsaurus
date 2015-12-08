set( BASE ${CMAKE_SOURCE_DIR}/contrib/z-lz-lzo )

set ( BASE ${CMAKE_SOURCE_DIR}/contrib/libs )

add_library( ytext-zstd STATIC
  ${BASE}/zstd/lib/fse.c
  ${BASE}/zstd/lib/zstd.c
)
