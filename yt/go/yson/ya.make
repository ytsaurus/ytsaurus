GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

SRCS(
    compat.go
    decoder.go
    error.go
    escape.go
    generic.go
    infer.go
    marshal.go
    reader.go
    reflect.go
    scanner.go
    struct_tag.go
    time.go
    unmarshal.go
    validate.go
    writer.go
    ypath.go
)

GO_TEST_SRCS(
    benchmark_test.go
    decoder_test.go
    escape_test.go
    generic_test.go
    infer_test.go
    marshal_test.go
    reader_test.go
    reflect_test.go
    scanner_test.go
    struct_tag_test.go
    time_test.go
    unmarshal_test.go
    validate_test.go
    writer_test.go
    ypath_test.go
)

GO_XTEST_SRCS(
    bugs_test.go
    infer_example_test.go
)

END()

RECURSE(
    gotest
    yson2json
)

IF (
    NOT
    OPENSOURCE
)
    # fuzz/crashers contain "a.yandex-team.ru"

    RECURSE(fuzz)
ENDIF()
