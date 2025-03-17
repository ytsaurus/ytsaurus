LIBRARY()

SRCS(
    GLOBAL logic.cpp
    builder.cpp
    remap.cpp
    iterator.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/common
)

END()
