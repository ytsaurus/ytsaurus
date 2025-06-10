LIBRARY()

SRCS(
    manager.cpp
    events.cpp
    merge.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/iterator
)

END()
