LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/dynamic_config
    yt/yt/library/server_program
    yt/yt/server/lib/cross_cluster_replicated_state
    yt/yt/server/lib/rpc_proxy
    yt/yt/ytlib
)

END()
