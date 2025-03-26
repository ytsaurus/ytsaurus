LIBRARY()

SRCS(
    chunks.cpp
    chunk_meta.cpp
)

PEERDIR(
    contrib/ydb/core/tx/columnshard/engines/scheme/abstract
    contrib/ydb/core/tx/columnshard/common
)

END()
