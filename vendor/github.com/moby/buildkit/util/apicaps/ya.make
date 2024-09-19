GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    caps.go
)

GO_TEST_SRCS(caps_test.go)

END()

RECURSE(
    gotest
    pb
)
