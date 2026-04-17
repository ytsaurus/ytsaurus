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
    library/cpp/yson/node
    yt/yql/providers/yt/fmr/table_data_service/local/impl
    yt/yql/providers/yt/fmr/table_data_service/server
    yt/yql/providers/yt/fmr/tvm/impl
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
