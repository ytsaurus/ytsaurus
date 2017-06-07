set(NAME deps-arcadia-contrib-libs-brotli)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/brotli)
set(SRCS
  ${BASE}/enc/backward_references.cc
  ${BASE}/enc/block_splitter.cc
  ${BASE}/enc/brotli_bit_stream.cc
  ${BASE}/enc/dictionary.cc
  ${BASE}/enc/encode.cc
  ${BASE}/enc/encode_parallel.cc
  ${BASE}/enc/entropy_encode.cc
  ${BASE}/enc/histogram.cc
  ${BASE}/enc/literal_cost.cc
  ${BASE}/enc/metablock.cc
  ${BASE}/enc/static_dict.cc
  ${BASE}/enc/streams.cc
  ${BASE}/enc/utf8_util.cc
  ${BASE}/dec/bit_reader.c
  ${BASE}/dec/decode.c
  ${BASE}/dec/dictionary.c
  ${BASE}/dec/huffman.c
  ${BASE}/dec/state.c
  ${BASE}/dec/streams.c
)

include_directories(${CMAKE_SOURCE_DIR})

add_library(${NAME} STATIC ${SRCS})

