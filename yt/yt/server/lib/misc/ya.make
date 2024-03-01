LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    address_helpers.cpp
    archive_reporter.cpp
    cluster_connection.cpp
    config.cpp
    disk_health_checker.cpp
    disk_change_checker.cpp
    fork_executor.cpp
    format_manager.cpp
    job_table_schema.cpp
    private.cpp
    profiling_helpers.cpp
    GLOBAL public.cpp
    restart_manager.cpp
    job_reporter.cpp
    job_report.cpp
)

PEERDIR(
    yt/yt/ytlib
    yt/yt/library/containers/disk_manager
    yt/yt/library/coredumper
    yt/yt/library/process
    yt/yt/library/ytprof
    yt/yt/library/ytprof/allocation_tag_profiler
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
