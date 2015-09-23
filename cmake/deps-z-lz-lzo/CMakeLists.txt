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

add_library( ytext-snappy STATIC
  ${BASE}/snappy/snappy.cc
  ${BASE}/snappy/snappy-sinksource.cc
  ${BASE}/snappy/snappy-stubs-internal.cc
  ${BASE}/snappy/snappy-c.cc
)

add_library( ytext-zlib STATIC
  ${BASE}/zlib/adler32.c
  ${BASE}/zlib/compress.c
  ${BASE}/zlib/crc32.c
  ${BASE}/zlib/deflate.c
  ${BASE}/zlib/gzclose.c
  ${BASE}/zlib/gzlib.c
  ${BASE}/zlib/gzread.c
  ${BASE}/zlib/gzwrite.c
  ${BASE}/zlib/infback.c
  ${BASE}/zlib/inffast.c
  ${BASE}/zlib/inflate.c
  ${BASE}/zlib/inftrees.c
  ${BASE}/zlib/trees.c
  ${BASE}/zlib/uncompr.c
  ${BASE}/zlib/zutil.c
)

# zlib uses util/ which resides in the project root.
include_directories(
  ${CMAKE_SOURCE_DIR}
)

add_library( ytext-zstd STATIC
  ${BASE}/zstd/lib/fse.c
  ${BASE}/zstd/lib/zstd.c
)

add_library( ytext-brotli STATIC
  ${BASE}/brotli/enc/backward_references.cc
  ${BASE}/brotli/enc/block_splitter.cc
  ${BASE}/brotli/enc/brotli_bit_stream.cc
  ${BASE}/brotli/enc/encode.cc
  ${BASE}/brotli/enc/encode_parallel.cc
  ${BASE}/brotli/enc/entropy_encode.cc
  ${BASE}/brotli/enc/histogram.cc
  ${BASE}/brotli/enc/literal_cost.cc
  ${BASE}/brotli/enc/metablock.cc
  ${BASE}/brotli/enc/static_dict.cc
  ${BASE}/brotli/enc/streams.cc
  ${BASE}/brotli/enc/utf8_util.cc
  ${BASE}/brotli/dec/bit_reader.c
  ${BASE}/brotli/dec/decode.c
  ${BASE}/brotli/dec/dictionary.c
  ${BASE}/brotli/dec/huffman.c
  ${BASE}/brotli/dec/state.c
  ${BASE}/brotli/dec/streams.c
)

set_target_properties( ytext-zlib PROPERTIES
  COMPILE_DEFINITIONS BUILD_ZLIB
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

set_target_properties( ytext-snappy PROPERTIES
  VERSION   1.0.4
  SOVERSION 1.0
)

set_target_properties( ytext-zlib PROPERTIES
  VERSION   1.2.3
  SOVERSION 1.2.3
)

set_target_properties( ytext-quicklz PROPERTIES
  VERSION   1.5.1
  SOVERSION 1.5.1
)
