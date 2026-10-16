GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    transaction_manager_ut.cpp
)

PEERDIR(
    library/cpp/json/yson
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/tables
    yt/yt/library/profiling/solomon
)

SIZE(SMALL)

END()

RECURSE(mock)
