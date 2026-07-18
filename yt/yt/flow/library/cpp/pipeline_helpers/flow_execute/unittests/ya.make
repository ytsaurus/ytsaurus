GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    flow_execute_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/pipeline_helpers/flow_execute
)

SIZE(SMALL)

END()
