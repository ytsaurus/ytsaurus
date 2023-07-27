G_BENCHMARK()

OWNER(
    g:yt
    prime
)

IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ENDIF()

IF (YT_TEAMCITY AND SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    compare.cpp
)

ALLOCATOR(YT)

PEERDIR(
    yt/yt/core
    yt/yt/library/syncmap
)

END()
