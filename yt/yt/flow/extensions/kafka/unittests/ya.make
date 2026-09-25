GTEST(unittester-flow-kafka)

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SIZE(SMALL)

SRCS(
    read_buffer_ut.cpp
    source_ut.cpp
    spec_ut.cpp
    write_queue_ut.cpp
)

PEERDIR(
    yt/yt/flow/extensions/kafka
    yt/yt/core/test_framework
)

END()
