GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    fetcher_ut.cpp
    joiner_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/computation
    yt/yt/flow/library/cpp/connectors/servicelog
    yt/yt/flow/library/cpp/controller/unittests/mock
    yt/yt/flow/library/cpp/controller
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
)

SIZE(SMALL)

END()
