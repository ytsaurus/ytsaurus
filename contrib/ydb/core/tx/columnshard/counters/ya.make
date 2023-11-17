LIBRARY()

SRCS(
    indexation.cpp
    scan.cpp
    engine_logs.cpp
    blobs_manager.cpp
    columnshard.cpp
    insert_table.cpp
    common_data.cpp
    splitter.cpp
)

PEERDIR(
    library/cpp/monlib/dynamic_counters
    contrib/ydb/core/tx/columnshard/counters/common
    contrib/ydb/core/base
)

END()
