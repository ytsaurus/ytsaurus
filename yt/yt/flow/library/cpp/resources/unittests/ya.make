GTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    registry_ut.cpp
    resource_controller_ut.cpp
)

PEERDIR(
    yt/yt/flow/library/cpp/resources
)

END()

RECURSE(
    mock
)
