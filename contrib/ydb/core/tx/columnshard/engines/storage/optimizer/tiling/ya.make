LIBRARY()

SRCS(
    GLOBAL tiling.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/changes/abstract
    contrib/ydb/library/intersection_tree
)

END()
