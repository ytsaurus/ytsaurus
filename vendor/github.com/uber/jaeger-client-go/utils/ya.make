GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    http_json.go
    localip.go
    rand.go
    rate_limiter.go
    reconnecting_udp_conn.go
    udp_client.go
    utils.go
)

GO_TEST_SRCS(
    http_json_test.go
    rate_limiter_test.go
    # reconnecting_udp_conn_test.go
    # udp_client_test.go
    utils_test.go
)

END()

RECURSE(gotest)
