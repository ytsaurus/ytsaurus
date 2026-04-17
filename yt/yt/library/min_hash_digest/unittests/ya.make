GTEST()

SRCS(
    min_hash_digest_ut.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/library/min_hash_digest
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(
        ya:fat
    )
ENDIF()

END()
