set( BASE ${CMAKE_SOURCE_DIR}/contrib/z-lz-lzo )

add_library( ytext-fastlz STATIC
  ${BASE}/fastlz.c
)

add_library( ytext-lz4 STATIC
  ${BASE}/lz4.c
  ${BASE}/lz4hc.c
)

add_library( ytext-minilzo STATIC
  ${BASE}/minilzo.c
  ${BASE}/minilzo_crc.c
)

add_library( ytext-quicklz STATIC
  ${BASE}/quicklz.c
)

set ( BASE ${CMAKE_SOURCE_DIR}/contrib/libs )

add_library( ytext-zstd STATIC
  ${BASE}/zstd/lib/fse.c
  ${BASE}/zstd/lib/zstd.c
)

set_target_properties( ytext-fastlz PROPERTIES
  VERSION   0.1.0
  SOVERSION 0.1
)

set_target_properties( ytext-lz4 PROPERTIES
  VERSION   0.71
  SOVERSION 0.1
)

set_target_properties( ytext-minilzo PROPERTIES
  VERSION   2.0.6
  SOVERSION 2.0
)

set_target_properties( ytext-quicklz PROPERTIES
  VERSION   1.5.1
  SOVERSION 1.5
)

set_target_properties( ytext-quicklz PROPERTIES
  VERSION   1.5.1
  SOVERSION 1.5.1
)
