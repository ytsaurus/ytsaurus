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
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/fmr/worker/server
    yt/yql/providers/yt/fmr/coordinator/client
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/job_launcher
    yt/yql/providers/yt/fmr/job_preparer/impl
    yt/yql/providers/yt/fmr/table_data_service/client/impl
    yt/yql/providers/yt/fmr/table_data_service/local/impl
    yt/yql/providers/yt/fmr/table_data_service/discovery/file
    yt/yql/providers/yt/fmr/tvm/impl
    yt/yql/providers/yt/fmr/yt_job_service/file
    yt/yql/providers/yt/fmr/yt_job_service/impl
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg
    yql/essentials/utils/log
    yql/essentials/core/file_storage/proto
)

RESOURCE(
    yt/yql/providers/yt/fmr/cfg/local/fs.conf fs.conf
)

YQL_LAST_ABI_VERSION()

END()
