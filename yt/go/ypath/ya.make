GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    codecs.go
    lexer.go
    rich.go
    ypath.go
)

GO_TEST_SRCS(
    codecs_test.go
    lexer_test.go
    rich_test.go
    ypath_test.go
)

END()

RECURSE(gotest)
