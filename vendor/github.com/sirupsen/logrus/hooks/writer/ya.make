GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.9.3)

SRCS(
    writer.go
)

GO_TEST_SRCS(writer_test.go)

END()

RECURSE(
    gotest
)
