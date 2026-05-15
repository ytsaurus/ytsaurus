PROGRAM(run_vanilla)

ALLOCATOR(J)

SRCS(
    main.cpp
)

IF (OS_LINUX)
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

PEERDIR(
    library/cpp/getopt/small
    yt/cpp/mapreduce/client
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/coordinator/server
    yt/yql/providers/yt/fmr/coordinator/yt_coordinator_service/impl
    yt/yql/providers/yt/fmr/gc_service/impl
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/job_launcher
    yt/yql/providers/yt/fmr/job_preparer/interface
    yt/yql/providers/yt/fmr/table_data_service/local/impl
    yt/yql/providers/yt/fmr/table_data_service/server
    yt/yql/providers/yt/fmr/vanilla/coordinator_client
    yt/yql/providers/yt/fmr/vanilla/http_mon
    yt/yql/providers/yt/fmr/vanilla/peer_tracker
    yt/yql/providers/yt/fmr/vanilla/tds_discovery
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/fmr/yt_job_service/impl
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg
    yql/essentials/core/file_storage
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
)

YQL_LAST_ABI_VERSION()

END()
