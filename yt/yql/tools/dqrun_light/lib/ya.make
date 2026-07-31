LIBRARY()

SRCS(
    dqrun_light_lib.cpp
    dqrun_tool_base.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/actors/input_transforms
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/dq/helper
    contrib/ydb/library/yql/providers/dq/interface
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/provider/exec
    contrib/ydb/library/yql/providers/yt/actors
    contrib/ydb/library/yql/providers/yt/dq_task_preprocessor
    contrib/ydb/library/yql/utils/bindings
    yql/essentials/core/cbo
    yql/essentials/core/dq_integration
    yql/essentials/core/dq_integration/transform
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/computation
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/metrics
    yql/essentials/providers/common/provider
    yql/essentials/sql/settings
    yql/essentials/tools/yql_facade_run
    yql/essentials/utils/log
    yt/yql/providers/dq/gateway
    yt/yql/providers/dq/local_gateway
    yt/yql/providers/yt/comp_nodes/dq
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/mkql_dq
    yt/yql/providers/yt/provider
    yt/yql/tools/ytrun/lib
)

YQL_LAST_ABI_VERSION()

SUPPRESSIONS(
    lsan.supp
)

END()
