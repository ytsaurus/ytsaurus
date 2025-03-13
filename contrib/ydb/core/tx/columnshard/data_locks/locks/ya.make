LIBRARY()

SRCS(
    abstract.cpp
    list.cpp
    snapshot.cpp
    composite.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/storage
)

END()
