LIBRARY()

PEERDIR(
    library/cpp/deprecated/split
    contrib/ydb/library/yql/parser/pg_wrapper/interface
    contrib/ydb/library/yql/sql/settings
    contrib/ydb/library/yql/sql/v0
    contrib/ydb/library/yql/sql/v0/lexer
    contrib/ydb/library/yql/sql/v1
    contrib/ydb/library/yql/sql/v1/format
    contrib/ydb/library/yql/sql/v1/lexer
    contrib/ydb/library/yql/sql/v1/proto_parser
    contrib/ydb/library/yql/utils
)

SRCS(
    cluster_mapping.cpp
    sql.cpp
)

END()

RECURSE(
    pg
    pg_dummy
    settings
    v0
    v1
)
