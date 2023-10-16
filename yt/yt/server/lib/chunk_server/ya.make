LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    immutable_chunk_meta.cpp

    proto/job.proto
)

PEERDIR(
    yt/yt/ytlib
)

END()

RECURSE_FOR_TESTS(
    unittests
)
