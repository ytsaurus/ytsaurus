LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/flow/flow.make.inc)

SRCS(
    async_at_most_once_sink_base.cpp
    delegating_async_sink_base.cpp
    flow_queue_meta.cpp
    ordered_async_sink_base.cpp
    ordered_batching_async_sink_base.cpp
    ordered_source_base.cpp
    ordered_source.cpp
    sink_base.cpp
    sink_controller_base.cpp
    source_base.cpp
    source_controller_base.cpp
    sync_sink_base.cpp
)

PEERDIR(
    yt/yt/library/profiling/sensors_owner
    yt/yt/flow/library/cpp/common
    yt/yt/flow/library/cpp/misc
    library/cpp/digest/md5
    library/cpp/yt/misc
)

END()

RECURSE_FOR_TESTS(
    unittests
)
