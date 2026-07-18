LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    authentication.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt_proto/yt/flow/controller/proto
)

END()
