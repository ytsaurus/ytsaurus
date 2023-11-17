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
    library/cpp/actors/core
    contrib/ydb/core/tablet_flat
)

END()
