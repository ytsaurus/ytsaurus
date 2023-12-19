GO_LIBRARY()

LICENSE(MIT)

SRCS(
    labelvalues.go
    space.go
)

GO_TEST_SRCS(
    labelvalues_test.go
    space_test.go
)

END()

RECURSE(gotest)
