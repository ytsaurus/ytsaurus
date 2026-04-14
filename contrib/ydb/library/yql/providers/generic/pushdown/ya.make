LIBRARY()

SRCS(
    yql_generic_match_predicate.cpp
)

YQL_LAST_ABI_VERSION()

PEERDIR(
    contrib/ydb/library/yql/providers/generic/connector/api/service/protos
    contrib/ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
