GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    decoder.go
    encoder.go
    reader.go
    schema.go
    types.go
    writer.go
)

GO_TEST_SRCS(
    benchmark_test.go
    decoder_test.go
    encoder_test.go
    schema_test.go
)

END()

RECURSE(gotest)
