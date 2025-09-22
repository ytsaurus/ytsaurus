LIBRARY()

SRCS(
    blob.cpp
    blob_serialization.cpp
    header.cpp
    type_codecs_defs.cpp
)



PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/library/logger
)

END()

RECURSE_FOR_TESTS(
    ut
)
