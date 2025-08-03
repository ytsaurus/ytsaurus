LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    asan_warning_filter.cpp
    core_watcher.cpp
    cpu_monitor.cpp
    environment.cpp
    job_throttler.cpp
    job_detail.cpp
    job_prober_service.cpp
    job_profiler.cpp
    job_proxy.cpp
    memory_tracker.cpp
    merge_job.cpp
    partition_job.cpp
    partition_sort_job.cpp
    private.cpp
    program.cpp
    public.cpp
    remote_copy_job.cpp
    shallow_merge_job.cpp
    simple_sort_job.cpp
    sorted_merge_job.cpp
    stderr_writer.cpp
    tmpfs_manager.cpp
    user_job.cpp
    user_job_synchronizer_service.cpp
    user_job_write_controller.cpp
    trace_event_processor.cpp
    trace_consumer.cpp
)

PEERDIR(
    library/cpp/getopt
    library/cpp/yt/phdr_cache
    library/cpp/yt/mlock
    yt/yt/server/lib

    # TODO(levysotsky): Remove together with renaming code in job proxy.
    yt/yt/server/lib/controller_agent
    yt/yt/library/containers
    yt/yt/library/containers/cri
    yt/yt/server/lib/exec_node
    yt/yt/server/lib/rpc_proxy
    yt/yt/server/lib/shell
    yt/yt/server/lib/user_job
    yt/yt/server/tools
    yt/yt/library/sparse_coredump
    yt/yt/library/query/row_comparer
    yt/yt/library/profiling/solomon
    yt/yt/library/tracing/jaeger
    yt/yt/library/server_program
    yt/yt/library/signals
)

END()

IF (NOT OPENSOURCE)
    RECURSE(
        bin
    )
ENDIF()

RECURSE_FOR_TESTS(
    unittests
)
