LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    address_helpers.cpp
    archive_reporter.cpp
    cluster_throttlers_config.cpp
    config.cpp
    disk_health_checker.cpp
    estimate_size_helpers.cpp
    fork_executor.cpp
    format_manager.cpp
    interned_attributes.cpp
    job_report.cpp
    job_reporter.cpp
    job_table_schema.cpp
    private.cpp
    profiling_helpers.cpp
    GLOBAL public.cpp
    restart_manager.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
    yt/yt/ytlib/distributed_throttler
    yt/yt/library/coredumper
    yt/yt/library/process
    yt/yt/library/tcmalloc
    yt/yt/library/ytprof
    yt/yt/library/ytprof/allocation_tag_profiler
    yt/yt/library/containers/cri
    yt/yt/contrib/gogoproto
)

IF(OS_LINUX)
    PEERDIR(
        library/cpp/porto
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    unittests
)
