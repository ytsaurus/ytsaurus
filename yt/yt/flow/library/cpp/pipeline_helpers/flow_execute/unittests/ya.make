GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    direct_flow_execute_ut.cpp
    flow_execute_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    yt/yt/client/unittests/mock
    yt/yt/core/test_framework
    yt/yt/flow/library/cpp/client
    yt/yt/flow/library/cpp/misc
    yt/yt/flow/library/cpp/pipeline_helpers/flow_execute
    yt/yt_proto/yt/flow/controller/proto
)

SIZE(MEDIUM)

END()
