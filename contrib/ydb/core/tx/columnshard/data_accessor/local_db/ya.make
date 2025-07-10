LIBRARY()

SRCS(
    manager.cpp
    GLOBAL constructor.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/data_accessor/abstract
)

END()
