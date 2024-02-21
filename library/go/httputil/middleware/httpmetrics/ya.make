GO_LIBRARY()

SRCS(
    middleware.go
    middleware_opts.go
)

GO_TEST_SRCS(middleware_test.go)

IF (NOT OPENSOURCE)
    GO_XTEST_SRCS(
        example_test.go # depends on library/go/yandex
    )
ENDIF()

END()

RECURSE(gotest)
