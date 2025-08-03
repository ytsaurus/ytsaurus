PROGRAM(fmrrun)

ALLOCATOR(J)

SRCS(
    fmrrun.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/yql/essentials/tools/exports.symlist)
ENDIF()

IF (OOM_HELPER)
    PEERDIR(yql/essentials/utils/oom_helper)
ENDIF()

PEERDIR(
    yt/yql/tools/fmrrun/lib

    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/terminate_policy
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
