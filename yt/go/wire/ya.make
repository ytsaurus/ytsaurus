GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    decode.go
    encode.go
    reader.go
    row.go
    writer.go
)

GO_TEST_SRCS(
    decode_test.go
    encode_test.go
    row_test.go
)

END()

RECURSE(gotest)
