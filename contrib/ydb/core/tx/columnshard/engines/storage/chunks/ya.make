LIBRARY()

SRCS(
    data.cpp
    column.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/splitter/abstract
    contrib/ydb/core/tx/columnshard/splitter
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/columnshard/counters
)

END()
