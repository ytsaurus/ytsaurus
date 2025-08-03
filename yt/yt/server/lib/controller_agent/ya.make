LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PROTO_NAMESPACE(yt)

SRCS(
    statistics.cpp
    structs.cpp
    progress_counter.cpp
    read_range_registry.cpp
    job_size_constraints.cpp
    helpers.cpp
    public.cpp
    job_report.cpp
    network_project.cpp

    proto/job_tracker_service.proto
)

PEERDIR(
    yt/yt/ytlib

    # TODO(max42): eliminate.
    yt/yt/server/lib/scheduler
    yt/yt/server/lib/job_agent
)

END()
