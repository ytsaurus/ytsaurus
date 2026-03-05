GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v0.13.5-0.20251024222203-75eaa193e329)

SRCS(
    gateway.go
    server.go
)

GO_XTEST_SRCS(
    delta_test.go
    gateway_test.go
    server_test.go
)

END()

RECURSE(
    gotest
)
