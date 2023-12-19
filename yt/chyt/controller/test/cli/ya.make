GO_LIBRARY()

SIZE(MEDIUM)

IF (NOT OPENSOURCE)
    INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)
ENDIF()

DEPENDS(yt/python/yt/wrapper/bin/yt_make)

SRCS(
    cli.go
)

IF (OPENSOURCE)
    SRCS(
        yt_binary.go
    )
ENDIF()

IF (NOT OPENSOURCE)
    SRCS(
        yt_binary_internal.go
    )
ENDIF()

GO_XTEST_SRCS(cli_test.go)

END()

RECURSE(gotest)
