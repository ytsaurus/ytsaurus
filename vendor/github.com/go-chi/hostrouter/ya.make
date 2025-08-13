GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.3.0)

SRCS(
    hostrouter.go
)

GO_TEST_SRCS(hostrouter_test.go)

END()

RECURSE(
    gotest
)
