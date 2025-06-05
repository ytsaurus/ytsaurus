LIBRARY()

SRCS(
    manager.cpp
    events.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
