set(NAME deps-arcadia-contrib-libs-brotli)
set(BASE ${CMAKE_SOURCE_DIR}/contrib/libs/brotli)
set(SRCS
  ${BASE}/common/dictionary.c
  ${BASE}/enc/backward_references.c
  ${BASE}/enc/backward_references_hq.c
  ${BASE}/enc/bit_cost.c
  ${BASE}/enc/block_splitter.c
  ${BASE}/enc/brotli_bit_stream.c
  ${BASE}/enc/cluster.c
  ${BASE}/enc/compress_fragment.c
  ${BASE}/enc/compress_fragment_two_pass.c
  ${BASE}/enc/dictionary_hash.c
  ${BASE}/enc/encode.c
  ${BASE}/enc/entropy_encode.c
  ${BASE}/enc/histogram.c
  ${BASE}/enc/literal_cost.c
  ${BASE}/enc/memory.c
  ${BASE}/enc/metablock.c
  ${BASE}/enc/static_dict.c
  ${BASE}/enc/utf8_util.c
  ${BASE}/dec/bit_reader.c
  ${BASE}/dec/decode.c
  ${BASE}/dec/huffman.c
  ${BASE}/dec/state.c
)

include_directories(
  ${CMAKE_SOURCE_DIR}
  ${BASE}/include
)

add_library(${NAME} STATIC ${SRCS})

