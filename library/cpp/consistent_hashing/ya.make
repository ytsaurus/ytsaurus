LIBRARY(consistent-hashing)

SRCS(
    consistent_hashing.cpp
)

PEERDIR_FROM_POLICY(
    ALLOW
    contrib/clickhouse
    contrib/python/chdb
)

END()
