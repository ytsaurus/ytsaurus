LIBRARY()

SRCS(
    update.cpp
)

PEERDIR(
    contrib/ydb/core/tx/schemeshard/olap/operations/alter/abstract
    contrib/ydb/core/tx/columnshard/data_sharing/initiator/controller
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/public/sdk/cpp/src/client/types/credentials
)

YQL_LAST_ABI_VERSION()

END()
