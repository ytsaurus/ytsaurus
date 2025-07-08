LIBRARY()

SRCS(
    checksum.cpp
    encryption.cpp
    metadata.cpp
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/json
    contrib/ydb/core/backup/common/proto
    contrib/ydb/core/base
    contrib/ydb/core/util
    contrib/ydb/library/yverify_stream
)

GENERATE_ENUM_SERIALIZATION(metadata.h)

END()
