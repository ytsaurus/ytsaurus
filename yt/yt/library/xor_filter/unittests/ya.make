GTEST()

SRCS(
    xor_filter_ut.cpp
)

PEERDIR(
    yt/yt/library/xor_filter
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    TAG(
        ya:fat
    )
ENDIF()
    
END()
