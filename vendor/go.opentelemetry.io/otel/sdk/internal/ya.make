GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    gen.go
    internal.go
)

END()

RECURSE(
    env
    internaltest
    matchers
)
