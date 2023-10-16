LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    public.cpp
    config.cpp
    exec_node_descriptor.cpp
    experiments.cpp
    scheduling_tag.cpp
    structs.cpp
    event_log.cpp
    helpers.cpp
    job_metrics.cpp
    resource_metering.cpp

    proto/allocation_tracker_service.proto
    proto/controller_agent_tracker_service.proto
)

PEERDIR(
    yt/yt/orm/library/query
    yt/yt/ytlib
    yt/yt/server/lib/node_tracker_server
    yt/yt_proto/yt/client
)

END()
