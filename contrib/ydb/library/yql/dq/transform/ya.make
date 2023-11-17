LIBRARY()

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/library/yql/dq/integration/transform
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/utils
)

SRCS(
    yql_common_dq_transform.cpp
)

YQL_LAST_ABI_VERSION()


END()
