GO_LIBRARY()

LICENSE(MIT)

SRCS(
    writer.go
)

GO_TEST_SRCS(writer_test.go)

END()

RECURSE(
    gotest
)
