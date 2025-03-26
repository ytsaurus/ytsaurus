LIBRARY()

SRCS(
    GLOBAL constructor.cpp
    read_metadata.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/reader/abstract
    contrib/ydb/core/tx/columnshard/engines/reader/common_reader/constructor
    contrib/ydb/core/kqp/compute_actor
)

END()
