PROGRAM(run_coordinator_server)

ALLOCATOR(J)

SRCS(
    yql_yt_coordinator_service.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/yson/node
    yt/yql/providers/yt/fmr/coordinator/server
    yt/yql/providers/yt/fmr/coordinator/impl
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
