LIBRARY()

SRCS(
    queries.cpp
    db_queries_maker.cpp
)

PEERDIR(
    contrib/ydb/core/ymq/base
)

END()
