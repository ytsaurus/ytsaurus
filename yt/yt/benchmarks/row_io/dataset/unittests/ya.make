GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    multi_chunk_writer_ut.cpp
)

PEERDIR(
    yt/yt/benchmarks/row_io/dataset
    yt/yt/core/test_framework
    yt/yt/ytlib
)

SIZE(SMALL)

END()
