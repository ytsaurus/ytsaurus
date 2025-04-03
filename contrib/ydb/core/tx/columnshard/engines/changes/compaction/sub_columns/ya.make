LIBRARY()

SRCS(
    GLOBAL logic.cpp
    builder.cpp
    remap.cpp
    iterator.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/changes/compaction/common
    contrib/ydb/core/formats/arrow/accessor/sub_columns
)

END()
