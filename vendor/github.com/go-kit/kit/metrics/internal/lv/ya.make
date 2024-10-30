GO_LIBRARY()

LICENSE(MIT)

VERSION(v0.13.0)

SRCS(
    labelvalues.go
    space.go
)

GO_TEST_SRCS(
    labelvalues_test.go
    space_test.go
)

END()

RECURSE(
    gotest
)
