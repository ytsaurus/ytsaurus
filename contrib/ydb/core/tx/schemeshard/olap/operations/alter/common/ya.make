LIBRARY()

SRCS(
    update.cpp
    object.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()
