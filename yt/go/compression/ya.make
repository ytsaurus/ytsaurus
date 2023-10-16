GO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

DEPENDS(yt/yt/tools/run_codec)

SRCS(
    brotli.go
    codec.go
    lz.go
    snappy.go
    zlib.go
    zstd.go
)

IF (NOT OPENSOURCE)
    GO_TEST_SRCS(
        codec_test.go
    )
ENDIF()

END()

RECURSE(gotest)
