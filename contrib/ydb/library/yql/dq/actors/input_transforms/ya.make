LIBRARY()

SRCS(
    dq_input_transform_lookup.cpp
    dq_input_transform_lookup_factory.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    yql/essentials/minikql
    contrib/ydb/library/yql/dq/actors/compute
)

YQL_LAST_ABI_VERSION()

END()
