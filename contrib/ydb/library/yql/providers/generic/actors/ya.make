LIBRARY()

SRCS(
    yql_generic_read_actor.cpp
    yql_generic_source_factory.cpp
)

PEERDIR(
    contrib/ydb/library/yql/dq/actors/compute
    contrib/ydb/library/yql/minikql/computation/llvm
    contrib/ydb/library/yql/providers/common/token_accessor/client
    contrib/ydb/library/yql/providers/generic/proto
    contrib/ydb/library/yql/public/types
    contrib/ydb/library/yql/providers/generic/connector/libcpp
)

YQL_LAST_ABI_VERSION()

END()
