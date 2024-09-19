GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    multireader.go
    multiwriter.go
    progress.go
)

GO_TEST_SRCS(progress_test.go)

END()

RECURSE(
    gotest
)
