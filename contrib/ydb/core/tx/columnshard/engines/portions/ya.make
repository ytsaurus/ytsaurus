LIBRARY()

SRCS(
    portion_info.cpp
    column_record.cpp
    with_blobs.cpp
    meta.cpp
    common.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/tx/columnshard/splitter
    contrib/ydb/core/tx/columnshard/common
    contrib/ydb/core/tablet_flat
)

GENERATE_ENUM_SERIALIZATION(portion_info.h)

END()
