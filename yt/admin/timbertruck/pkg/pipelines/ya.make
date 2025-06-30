GO_LIBRARY()

SRCS(
    decompressor.go
    file.go
    followingfile.go
    pipelines.go
    textpipeline.go
    types.go
)

GO_XTEST_SRCS(
    pipeline_test.go
    textpipeline_test.go
)

END()

RECURSE_FOR_TESTS(
    gotest
)
