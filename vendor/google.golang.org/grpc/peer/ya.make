GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.69.4)

SRCS(
    peer.go
)

GO_TEST_SRCS(peer_test.go)

END()

RECURSE(
    gotest
)
