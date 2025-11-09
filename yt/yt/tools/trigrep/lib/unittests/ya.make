GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    zstd_reader_ut.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework

    yt/yt/tools/trigrep/lib
)

END()
