GTEST(ytflow-query-engine-portable-ut)

INCLUDE(../common.make.inc)

SRCS(
    unsupported_expression_ut.cpp
    worker_initialization_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/misc/testing
    yt/yt/flow/library/cpp/runner/no_engine
    yt/yt/library/query/portable_engine
)

END()
