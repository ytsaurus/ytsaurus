GO_LIBRARY()

SRCS(
    attributes.go
    config.go
    root.go
    utils.go
)

GO_TEST_SRCS(
    attributes_test.go
    root_test.go
)

END()

RECURSE(
    gotest
)
