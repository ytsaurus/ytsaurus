LIBRARY()

SRCS(
    tx_general.cpp
    tx_save_progress.cpp
    tx_save_state.cpp
    tx_remove.cpp
    tx_add.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/columnshard/bg_tasks/abstract
    contrib/ydb/core/tx/columnshard/bg_tasks/events
    contrib/ydb/core/tx/columnshard/bg_tasks/session
    contrib/ydb/core/protos
)

END()
