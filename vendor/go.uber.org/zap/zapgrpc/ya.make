GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.27.1)

SRCS(
    zapgrpc.go
)

GO_TEST_SRCS(zapgrpc_test.go)

END()

RECURSE(
    gotest
)
