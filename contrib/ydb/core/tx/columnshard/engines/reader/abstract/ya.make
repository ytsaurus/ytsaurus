LIBRARY()

SRCS(
    abstract.cpp
    read_metadata.cpp
    constructor.cpp
    read_context.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
    contrib/ydb/core/tx/columnshard/engines/insert_table
    contrib/ydb/core/tx/program
    contrib/ydb/core/protos
    contrib/ydb/core/tx/columnshard/data_sharing/protos
)

GENERATE_ENUM_SERIALIZATION(read_metadata.h)

END()
