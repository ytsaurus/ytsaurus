LIBRARY()

LICENSE(
    Apache-2.0 AND
    Public-Domain
)

LICENSE_TEXTS(.yandex_meta/licenses.list.txt)

VERSION(2019-03-11)

ORIGINAL_SOURCE(https://github.com/google/open-vcdiff/archive/868f459a8d815125c2457f8c74b12493853100f9.tar.gz)

NO_UTIL()

SRCDIR(contrib/tools/open-vcdiff/src)

SRCS(
    addrcache.cc
    blockhash.cc
    codetable.cc
    decodetable.cc
    encodetable.cc
    headerparser.cc
    instruction_map.cc
    jsonwriter.cc
    logging.cc
    varint_bigendian.cc
    vcdecoder.cc
    vcdiffengine.cc
    vcencoder.cc
)

ADDINCL(
    GLOBAL contrib/tools/open-vcdiff/src
    contrib/tools/open-vcdiff/src/google
)

PEERDIR(
    contrib/libs/zlib
)

END()

RECURSE_FOR_TESTS(
    bin
    ut
)
