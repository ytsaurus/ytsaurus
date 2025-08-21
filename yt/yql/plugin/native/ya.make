LIBRARY()

SRCS(
    GLOBAL plugin.cpp
    error_helpers.cpp
    progress_merger.cpp
    provider_load.cpp
    dq_manager.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/resource
    library/cpp/yson
    library/cpp/yson/node
    library/cpp/malloc/system
    yt/cpp/mapreduce/client
    yt/cpp/mapreduce/common
    yt/yt/client
    yt/yt/library/program
    yql/essentials/ast
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/http_download
    yql/essentials/core/progress_merger
    yql/essentials/core/services/mounts
    yql/essentials/core/user_data
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/protos
    yql/essentials/public/langver
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/utils/backtrace
    yql/essentials/utils/log
    yql/essentials/utils/log/proto
    yql/essentials/sql/v1
    yql/essentials/sql/v1/lexer/antlr4
    yql/essentials/sql/v1/lexer/antlr4_ansi
    yql/essentials/sql/v1/proto_parser/antlr4
    yql/essentials/sql/v1/proto_parser/antlr4_ansi
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/solomon/gateway
    contrib/ydb/library/yql/providers/solomon/provider
    yql/essentials/core
    yql/essentials/core/url_preprocessing
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/opt
    contrib/ydb/library/yql/providers/dq/actors/yt
    contrib/ydb/library/yql/providers/dq/global_worker_manager
    contrib/ydb/library/yql/providers/dq/helper
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/provider/exec
    contrib/ydb/library/yql/providers/dq/service
    contrib/ydb/library/yql/providers/dq/stats_collector
    contrib/ydb/library/yql/providers/dq/worker_manager/interface
    yt/yql/providers/yt/gateway/native
    yt/yql/providers/yt/codec
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/lib/log
    yt/yql/providers/yt/lib/res_pull
    yt/yql/providers/yt/lib/row_spec
    yt/yql/providers/yt/lib/schema
    yt/yql/providers/yt/lib/skiff
    yt/yql/providers/yt/lib/yt_download
    yt/yql/providers/yt/provider
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/dq/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
    contrib/ydb/library/yql/providers/yt/dq_task_preprocessor

    yt/yql/plugin
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ELSE()
    SRCS(
        dummy_secret_masker.cpp
        no_ytflow_load.cpp
    )

    PEERDIR(
        yt/yql/providers/yt/lib/secret_masker/dummy
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
