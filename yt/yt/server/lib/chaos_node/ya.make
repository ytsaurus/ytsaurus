LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    replication_card_watcher_service_callbacks.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
    yt/yt_proto/yt/client
)

END()
