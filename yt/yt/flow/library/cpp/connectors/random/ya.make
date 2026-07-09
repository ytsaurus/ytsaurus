LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    source.cpp
    spec.cpp
    GLOBAL register.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/connectors/common
    yt/yt/flow/library/cpp/resources
    yt/yt/core
    yt/yt/client
)

END()
