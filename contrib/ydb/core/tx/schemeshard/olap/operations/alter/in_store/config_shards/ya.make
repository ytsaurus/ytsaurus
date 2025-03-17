LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
)

YQL_LAST_ABI_VERSION()

END()
