LIBRARY()

YQL_LAST_ABI_VERSION()

PEERDIR(
    library/cpp/iterator

    yql/essentials/ast
    yql/essentials/core/expr_nodes
    yql/essentials/minikql
    yql/essentials/providers/common/mkql
    yql/essentials/utils

    yt/yql/providers/yt/lib/lambda_builder
)

SRCS(
    yql_ytflow_lambda_builder.cpp
)

END()
