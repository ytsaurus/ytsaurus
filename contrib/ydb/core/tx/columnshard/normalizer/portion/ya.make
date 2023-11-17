LIBRARY()

SRCS(
    normalizer.cpp
    min_max.cpp
    chunks.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/normalizer/abstract
    contrib/ydb/core/tx/columnshard/blobs_reader
    contrib/ydb/core/tx/columnshard/engines/portions
    contrib/ydb/core/tx/conveyor/usage
)

END()
