LIBRARY(ytserver)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    # TODO(max42): move these to particular targets.
    contrib/libs/openssl
    contrib/libs/sparsehash
    library/cpp/streams/brotli
    library/cpp/streams/lz/lz4
    library/cpp/streams/lz/snappy
    library/cpp/porto/proto
    yt/yt/library/re2
    yt/yt/library/auth
    yt/yt/library/query/engine
    yt/yt/library/query/row_comparer
    yt/yt/core/rpc/grpc
    yt/yt/ytlib
    yt/yt/library/auth_server
    yt/yt_proto/yt/client

    # TODO(max42): shared by literally everybody for now. Maybe move to a combined target?
    yt/yt/server/lib/admin
    yt/yt/library/coredumper

    # TODO(max42): this target is a mess; re-visit it.
    yt/yt/server/lib/misc
)

END()

RECURSE(
    admin
    cell_server
    cellar_agent
    chaos_cache
    chaos_node
    chunk_pools
    chunk_server
    controller_agent
    cypress_election
    cypress_registrar
    discovery_server
    election
    exec_node
    hive
    hydra
    incumbent_client
    io
    job_agent
    job_proxy
    logging
    lsm
    misc
    nbd
    node_tracker_server
    rpc_proxy
    scheduler
    security_server
    shell
    table_server
    tablet_balancer
    tablet_node
    tablet_server
    timestamp_server
    transaction_server
    transaction_supervisor
    user_job
    zookeeper_master
    zookeeper_proxy
)
