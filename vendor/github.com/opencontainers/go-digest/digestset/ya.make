GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    CC-BY-SA-4.0
)

SRCS(
    set.go
)

GO_TEST_SRCS(set_test.go)

END()

RECURSE(
    gotest
)
