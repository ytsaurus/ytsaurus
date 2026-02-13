GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SUBSCRIBER(g:yt)

ALLOCATOR(TCMALLOC)

SRCS(
    chunk_reader_writer_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/s3

    yt/yt/ytlib

    library/cpp/digest/md5
)

INCLUDE(${ARCADIA_ROOT}/yt/yt/tests/local_s3_recipe/recipe.inc)

END()
