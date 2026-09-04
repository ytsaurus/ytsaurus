LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/threading/future

    yql/essentials/minikql
    yql/essentials/minikql/computation

    yt/yql/providers/ytflow/integration/mkql_interface
)

SRCS(
    mkql_ytflow_chunked_forward_list.cpp
    mkql_ytflow_lookup_join.cpp
)

END()

RECURSE_FOR_TESTS(
    ut
)
