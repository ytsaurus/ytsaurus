GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.71.0)

SRCS(
    peer.go
)

GO_TEST_SRCS(peer_test.go)

END()

RECURSE(
    gotest
)
