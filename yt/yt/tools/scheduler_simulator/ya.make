LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    operation_description.cpp
    operation.cpp
    operation_controller.cpp
    private.cpp
    scheduler_strategy_host.cpp
    shared_data.cpp
    node_shard.cpp
    node_worker.cpp
    control_thread.cpp
    event_log.cpp
)

PEERDIR(
    yt/yt/server/scheduler
)

END()

RECURSE(
    bin
    unittests
)

IF (NOT OPENSOURCE)
    # NB: default-linux-x86_64-relwithdebinfo-opensource build does not support python programs and modules.
    RECURSE(
        scripts
    )
ENDIF()
