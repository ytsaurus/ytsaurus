LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/yt/memory

    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/providers/common/provider

    yt/yql/providers/ytflow/integration/mkql_interface

    yt/yt/client
    yt/yt/core
)

SRCS(
    yql_yt_ytflow_lookup_provider.cpp
    yql_yt_ytflow_schema.cpp
)

END()
