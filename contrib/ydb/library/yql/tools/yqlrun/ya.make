PROGRAM(yqlrun)

ALLOCATOR(J)

SRCS(
    yqlrun.cpp
    gateway_spec.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    contrib/libs/protobuf
    library/cpp/getopt
    library/cpp/yson
    library/cpp/svnversion
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/core/facade
    contrib/ydb/library/yql/core/file_storage
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/core/file_storage/http_download
    contrib/ydb/library/yql/core/services/mounts
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/utils/backtrace
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/providers/common/codec
    contrib/ydb/library/yql/providers/common/comp_nodes
    contrib/ydb/library/yql/providers/common/proto
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/providers/common/udf_resolve
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/yt/gateway/file
    contrib/ydb/library/yql/core/url_preprocessing
    contrib/ydb/library/yql/tools/yqlrun/http
    contrib/ydb/library/yql/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()
