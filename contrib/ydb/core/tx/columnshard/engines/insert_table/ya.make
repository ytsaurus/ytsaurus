LIBRARY()

SRCS(
    insert_table.cpp
    rt_insertion.cpp
    data.cpp
    path_info.cpp
    meta.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tablet_flat
)

END()
