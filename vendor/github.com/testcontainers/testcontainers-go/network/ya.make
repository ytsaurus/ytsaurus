GO_LIBRARY()

LICENSE(MIT)

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
