GO_LIBRARY()

LICENSE(BSD-3-Clause)

SRCS(
    descriptor.go
    descriptor.pb.go
    descriptor_gostring.gen.go
    helper.go
)

GO_XTEST_SRCS(descriptor_test.go)

END()

RECURSE(
    gotest
)
