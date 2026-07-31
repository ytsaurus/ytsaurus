LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    pipeline.cpp
)

PEERDIR(
    library/cpp/yt/logging

    yt/yt/flow/library/cpp/pipeline_helpers/flow_execute
    yt/yt/flow/library/cpp/common

    yt/yt/client
    yt/yt/core
    yt/yt/flow/library/cpp/native_client
)

END()
