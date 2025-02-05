LIBRARY()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/dq/actors/compute
    yql/essentials/minikql/computation
    yql/essentials/utils
)

SRCS(
    yql_common_dq_factory.cpp
)

YQL_LAST_ABI_VERSION()


END()

