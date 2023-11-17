UNITTEST_FOR(contrib/ydb/library/yql/core/arrow_kernels/registry)

PEERDIR(
)

YQL_LAST_ABI_VERSION()

SRCS(
    registry_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/core/arrow_kernels/request
    contrib/ydb/library/yql/public/udf
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/minikql/comp_nodes/llvm
    contrib/ydb/library/yql/udfs/common/url_base
    contrib/ydb/library/yql/udfs/common/json2
)

END()
