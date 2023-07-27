G_BENCHMARK()

OWNER(g:yt)

SIZE(MEDIUM)

IF (SANITIZER_TYPE)
    TAG(ya:not_autocheck)
ENDIF()

IF (YT_TEAMCITY AND SANITIZER_TYPE)
    TAG(ya:manual)
ENDIF()

SRCS(
    yson_struct_bench.cpp
)

PEERDIR(
    yt/yt/ytlib
)

END()
