LIBRARY()

SRCS(
    tablet.cpp
    tx_init.cpp
    tx_init_schema.cpp
)

PEERDIR(
    library/cpp/lwtrace/protos
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/protos
    contrib/ydb/core/scheme/protos
)

END()
