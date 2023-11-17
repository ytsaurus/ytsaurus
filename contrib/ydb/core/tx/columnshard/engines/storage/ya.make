LIBRARY()

SRCS(
    granule.cpp
    storage.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/engines/storage/optimizer
    contrib/ydb/core/formats/arrow
)

GENERATE_ENUM_SERIALIZATION(granule.h)

END()
