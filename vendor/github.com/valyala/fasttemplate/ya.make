GO_LIBRARY()

LICENSE(MIT)

VERSION(v1.2.2)

SRCS(
    template.go
    unsafe.go
)

GO_TEST_SRCS(
    example_test.go
    template_test.go
    template_timing_test.go
)

END()

RECURSE(
    gotest
)
