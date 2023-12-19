GO_LIBRARY()

LICENSE(Apache-2.0)

SRCS(
    binary_io.go
    handle.go
    json_io.go
    keyset.go
    manager.go
    mem_io.go
    reader.go
    validation.go
    writer.go
)

GO_XTEST_SRCS(
    binary_io_test.go
    handle_test.go
    json_io_test.go
    manager_test.go
    validation_test.go
)

END()

RECURSE(gotest)
