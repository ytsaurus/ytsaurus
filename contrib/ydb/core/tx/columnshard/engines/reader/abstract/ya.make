LIBRARY()

SRCS(
    abstract.cpp
    read_metadata.cpp
    constructor.cpp
    read_context.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/resource_pools
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
    contrib/ydb/core/tx/columnshard/data_sharing/protos
    contrib/ydb/core/tx/conveyor/usage
    contrib/ydb/core/tx/program
)

GENERATE_ENUM_SERIALIZATION(read_metadata.h)
YQL_LAST_ABI_VERSION()

END()
