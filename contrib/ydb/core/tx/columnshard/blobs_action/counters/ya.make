LIBRARY()

SRCS(
    read.cpp
    storage.cpp
    write.cpp
    remove_declare.cpp
    remove_gc.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/library/actors/core
    contrib/ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(storage.h)

END()
