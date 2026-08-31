GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    arrival_order_table_sink_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/resources/unittests/mock
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/common/unittests/mock
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/static_table
    yt/yt/client/hedging/unittests/mock
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

END()
