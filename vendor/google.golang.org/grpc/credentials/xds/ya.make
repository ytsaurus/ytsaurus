GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.74.2)

SRCS(
    xds.go
)

GO_TEST_SRCS(
    xds_client_test.go
    xds_server_test.go
)

END()

RECURSE(
    gotest
)
