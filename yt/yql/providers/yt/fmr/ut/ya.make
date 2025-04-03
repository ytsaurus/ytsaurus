UNITTEST()

SRCS(
    yql_fmr_ut.cpp
)

PEERDIR(
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/core
    yql/essentials/core/cbo/simple
    yql/essentials/core/facade
    yql/essentials/core/services
    yql/essentials/core/services/mounts
    yql/essentials/core/file_storage
    yql/essentials/providers/common/udf_resolve
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/core/type_ann
    yt/yql/providers/yt/lib/ut_common
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/schema/parser
    yql/essentials/providers/result/provider
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/gateway/fmr
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/fmr/coordinator/impl
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/job_factory/impl
    yt/yql/providers/yt/fmr/table_data_service/local
    yt/yql/providers/yt/fmr/worker/impl
    yt/yql/providers/yt/fmr/yt_service/impl
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/sql/pg
)

YQL_LAST_ABI_VERSION()

END()
