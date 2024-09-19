GO_LIBRARY()

LICENSE(MIT)

SRCS(
    doc.go
    scan.go
    stacks.go
)

GO_TEST_SRCS(
    scan_test.go
    stacks_test.go
)

END()

RECURSE(
    gotest
)
