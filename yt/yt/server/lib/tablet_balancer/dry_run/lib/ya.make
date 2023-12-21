LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    executor.cpp
    helpers.cpp
    holders.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/server/lib/tablet_balancer
    yt/yt/server/lib/tablet_node
    yt/yt/ytlib
)

END()
