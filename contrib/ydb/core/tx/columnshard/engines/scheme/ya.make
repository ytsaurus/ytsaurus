LIBRARY()

SRCS(
    abstract_scheme.cpp
    snapshot_scheme.cpp
    filtered_scheme.cpp
    index_info.cpp
    tier_info.cpp
    column_features.cpp
    schema_diff.cpp
    objects_cache.cpp
    schema_version.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow

    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/engines/scheme/indexes
    contrib/ydb/core/tx/columnshard/engines/scheme/abstract
    contrib/ydb/core/tx/columnshard/engines/scheme/versions
    contrib/ydb/core/tx/columnshard/engines/scheme/tiering
    contrib/ydb/core/tx/columnshard/engines/scheme/column
    contrib/ydb/core/tx/columnshard/engines/scheme/common
    contrib/ydb/core/tx/columnshard/engines/scheme/defaults
    contrib/ydb/core/formats/arrow/accessor
    contrib/ydb/core/tx/columnshard/blobs_action/abstract
)

YQL_LAST_ABI_VERSION()

END()
