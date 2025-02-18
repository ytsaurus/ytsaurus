PROGRAM(run_worker)

ALLOCATOR(J)

SRCS(
    yql_yt_worker_service.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/uri
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/fmr/coordinator/client
    yt/yql/providers/yt/fmr/job_factory/impl
)

YQL_LAST_ABI_VERSION()

END()
