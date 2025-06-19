LIBRARY()

SRCS(
    limits.cpp
    reverse_accessor.cpp
    scalars.cpp
    snapshot.cpp
    portion.cpp
    tablet_id.cpp
    blob.cpp
    volume.cpp
    path_id.cpp
)

PEERDIR(
    contrib/ydb/library/formats/arrow/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/common/protos
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/columnshard/transactions/protos
    contrib/ydb/core/tx/columnshard/export/protos
    contrib/ydb/core/scheme/protos
)

GENERATE_ENUM_SERIALIZATION(portion.h)

END()
