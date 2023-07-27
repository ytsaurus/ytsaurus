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

SIZE(MEDIUM)

SRCS(
    benchmark.cpp
)

ALLOCATOR(TCMALLOC_256K)

PEERDIR(
    yt/yt/library/ytprof
    yt/yt/core/crypto
)

END()
