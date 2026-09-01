GTEST()

VERSION(2019-03-11)

LICENSE(Apache-2.0)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

SRCDIR(contrib/tools/open-vcdiff/src)

NO_UTIL()

PEERDIR(
    contrib/tools/open-vcdiff
)

SRCS(
    addrcache_test.cc
    blockhash_test.cc
    codetable_test.cc
    decodetable_test.cc
    encodetable_test.cc
    headerparser_test.cc
    instruction_map_test.cc
    jsonwriter_test.cc
    output_string_test.cc
    rolling_hash_test.cc
    varint_bigendian_test.cc
    vcdecoder1_test.cc
    vcdecoder2_test.cc
    vcdecoder3_test.cc
    vcdecoder4_test.cc
    vcdecoder5_test.cc
    vcdecoder6_test.cc
    vcdecoder_test.cc
    vcdiffengine_test.cc
    vcencoder_test.cc
)

END()
