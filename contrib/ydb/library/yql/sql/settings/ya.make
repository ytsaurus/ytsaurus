LIBRARY()

SRCS(
    partitioning.cpp
    translation_settings.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/json
    contrib/ydb/library/yql/core/issue
    contrib/ydb/library/yql/core/issue/protos
    contrib/ydb/library/yql/utils
)

END()
