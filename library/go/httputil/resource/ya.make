GO_LIBRARY()

SRCS(
    buildinfo.go
    dir.go
    doc.go
    file.go
)

GO_XTEST_SRCS(
    dir_test.go
    example_test.go
)

END()

RECURSE(gotest)
