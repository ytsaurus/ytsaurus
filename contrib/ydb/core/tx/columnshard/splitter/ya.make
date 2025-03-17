LIBRARY()

SRCS(
    batch_slice.cpp
    chunks.cpp
    column_info.cpp
    settings.cpp
    blob_info.cpp
    chunk_meta.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/core/tx/columnshard/splitter/abstract
    contrib/ydb/core/tx/columnshard/engines/scheme
    contrib/ydb/core/formats/arrow/splitter
)

END()

RECURSE_FOR_TESTS(
    ut
)
