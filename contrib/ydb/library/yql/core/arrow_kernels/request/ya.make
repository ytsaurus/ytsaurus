LIBRARY()

SRCS(
    request.cpp
)

PEERDIR(
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/providers/common/mkql
    contrib/ydb/library/yql/core
    contrib/ydb/library/yql/sql
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(request.h)

END()
