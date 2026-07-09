LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    message_id_batch.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/common/worker/proto
)

END()

RECURSE_FOR_TESTS(
    unittests
)
