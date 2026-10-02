YQL_LIBRARY()

PEERDIR(
    library/cpp/threading/future

    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/utils
)

SRCS(
    yql_ytflow_lookup_provider.cpp
)

GENERATE_ENUM_SERIALIZATION(yql_ytflow_lookup_provider.h)

END()
