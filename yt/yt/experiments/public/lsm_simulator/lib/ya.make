LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    action_queue.cpp
    compaction.cpp
    helpers.cpp
    partition.cpp
    public.cpp
    row.cpp
    simulator.cpp
    statistics.cpp
    store.cpp
    store_manager.cpp
    structured_logger.cpp
    tablet.cpp
    optimizer.cpp
    mount_config_optimizer.cpp
    tabular_formatter.cpp
)

PEERDIR(
    yt/yt/server/lib/hydra
    yt/yt/server/lib/lsm
    library/cpp/getopt
    library/cpp/json
)

END()
