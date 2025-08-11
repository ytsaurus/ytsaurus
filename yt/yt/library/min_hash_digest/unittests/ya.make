GTEST()

SRCS(
    min_hash_digest_ut.cpp
)

PEERDIR(
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(
        ya:fat
    )
ENDIF()

END()
