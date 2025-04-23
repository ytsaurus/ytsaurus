GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

VERSION(v1.17.11)

SRCS(
    gzkp.go
)

GO_TEST_SRCS(gzkp_test.go)

END()

RECURSE(
    gotest
)
