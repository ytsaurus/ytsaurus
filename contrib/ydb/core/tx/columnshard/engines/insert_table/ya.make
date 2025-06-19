LIBRARY()

SRCS(
    insert_table.cpp
    rt_insertion.cpp
    user_data.cpp
    inserted.cpp
    committed.cpp
    path_info.cpp
    meta.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/formats/arrow/modifier
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tablet_flat
)

END()
