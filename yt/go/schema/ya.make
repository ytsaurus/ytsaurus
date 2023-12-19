GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    compare.go
    complex_type.go
    infer.go
    schema.go
    time.go
)

GO_TEST_SRCS(
    compare_test.go
    complex_type_test.go
    infer_test.go
    schema_test.go
    time_test.go
)

GO_XTEST_SRCS(infer_example_test.go)

END()

RECURSE(gotest)
