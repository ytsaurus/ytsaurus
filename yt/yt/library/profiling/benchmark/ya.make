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

ALLOCATOR(YT)

PEERDIR(
    yt/yt/library/profiling/solomon
)

END()
