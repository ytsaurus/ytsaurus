LIBRARY()

SRCS(
    tx_scan.cpp
    tx_internal_scan.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/engines/reader/actor
    contrib/ydb/core/tx/columnshard/engines/reader/sys_view/constructor
    contrib/ydb/core/tx/columnshard/engines/reader/plain_reader/constructor
)

END()
