GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.4.2)

SRCS(
    color.go
)

GO_TEST_SRCS(color_test.go)

END()

RECURSE(
    gotest
)
