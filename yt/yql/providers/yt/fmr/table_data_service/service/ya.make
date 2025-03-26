PROGRAM(run_table_data_service_server)

ALLOCATOR(J)

SRCS(
    yql_yt_table_service_bin.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    library/cpp/getopt
    yt/yql/providers/yt/fmr/table_data_service/server
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
