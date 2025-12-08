LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    cross_cluster_client.cpp
    cross_cluster_client_detail.cpp
    cross_cluster_replica_lock_waiter.cpp
    cross_cluster_replica_version.cpp
    cross_cluster_replicated_state.cpp
    cross_cluster_replicated_value.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
