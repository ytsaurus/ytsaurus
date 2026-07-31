UNITTEST_FOR(contrib/ydb/library/yql/providers/dq/provider)

SRCS(
    yql_dq_provider_ut.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/dq/comp_nodes
    contrib/ydb/library/yql/dq/transform
    contrib/ydb/library/yql/providers/dq/provider
    contrib/ydb/library/yql/providers/dq/provider/exec
    library/cpp/lwtrace
    library/cpp/lwtrace/mon
    library/cpp/testing/unittest
    yql/essentials/core/cbo/simple
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/services/mounts
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg
    yt/yql/providers/dq/local_gateway
    yt/yql/providers/yt/codec/codegen
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/lib/ut_common
    yt/yql/providers/yt/provider
)

YQL_LAST_ABI_VERSION()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
