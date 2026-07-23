GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.53.0)

SRCS(
    acme.go
    http.go
    jws.go
    rfc8555.go
    types.go
)

GO_TEST_SRCS(
    acme_test.go
    http_test.go
    jws_test.go
    rfc8555_test.go
    types_test.go
)

GO_XTEST_SRCS(
    # pebble_test.go
)

END()

RECURSE(
    autocert
    gotest
    internal
)
