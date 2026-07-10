GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    source_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/resources/unittests/mock
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/static_table_v2
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

END()
