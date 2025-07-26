LIBRARY()

SRCS(
    abstract_scheme.cpp
    snapshot_scheme.cpp
    filtered_scheme.cpp
    versioned_index.cpp
    preset_schemas.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/abstract
    contrib/ydb/core/tx/columnshard/engines/scheme/common
    contrib/ydb/core/tx/columnshard/data_sharing/protos
)

END()
