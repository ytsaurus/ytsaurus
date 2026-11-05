GTEST(unittester-tools-trigrep)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    index_builder_ut.cpp
    zstd_reader_ut.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/core/test_framework

    yt/yt/tools/trigrep/lib
)

END()
