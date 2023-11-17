LIBRARY()

SRCS(
    yql_hash_builder.cpp
    yql_op_hash.cpp
)

PEERDIR(
    contrib/libs/openssl
    contrib/ydb/library/yql/ast
    contrib/ydb/library/yql/utils
    contrib/ydb/library/yql/utils/log
    contrib/ydb/library/yql/core
)

END()
