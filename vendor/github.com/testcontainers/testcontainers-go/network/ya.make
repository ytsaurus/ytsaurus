GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.31.0)

SRCS(
    network.go
)

GO_XTEST_SRCS(
    # network_test.go
)

END()

RECURSE(
    gotest
)
