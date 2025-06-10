GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v1.36.0)

SRCS(
    gen.go
)

END()

RECURSE(
    baggage
    global
    internaltest
    matchers
)
