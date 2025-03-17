LIBRARY()

SRCS(
    kqp_workload_service.cpp
)

PEERDIR(
    contrib/ydb/core/cms/console

    contrib/ydb/core/fq/libs/compute/common

    contrib/ydb/core/kqp/workload_service/actors

    contrib/ydb/library/actors/interconnect
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    actors
    common
    tables
)

RECURSE_FOR_TESTS(
    ut
)
