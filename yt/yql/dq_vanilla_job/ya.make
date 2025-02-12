PROGRAM()

PEERDIR(
    library/cpp/svnversion
    library/cpp/yt/mlock
    contrib/ydb/library/yql/dq/comp_nodes
    yql/essentials/core/dq_integration/transform
    contrib/ydb/library/yql/dq/transform
    yql/essentials/providers/common/comp_nodes
    yt/yql/providers/yt/codec/codegen
    yql/essentials/minikql/comp_nodes/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
    yql/essentials/utils/backtrace
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/mkql_dq
    contrib/ydb/library/yql/tools/dq/worker_job
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

SRCS(
    main.cpp
)

END()
