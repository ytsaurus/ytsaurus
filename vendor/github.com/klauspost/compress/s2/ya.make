GO_LIBRARY()

LICENSE(
    Apache-2.0 AND
    BSD-3-Clause AND
    MIT
)

SRCS(
    decode.go
    decode_asm.go
    encode.go
    encode_all.go
    encode_best.go
    encode_better.go
    index.go
    s2.go
)

GO_TEST_SRCS(
    decode_test.go
    encode_test.go
    fuzz_test.go
    s2_test.go
)

GO_XTEST_SRCS(index_test.go)

IF (ARCH_X86_64)
    SRCS(
        decode_amd64.s
        encode_amd64.go
        encodeblock_amd64.go
        encodeblock_amd64.s
    )
ENDIF()

IF (OS_LINUX AND ARCH_ARM64)
    SRCS(
        decode_arm64.s
        encode_go.go
    )
ENDIF()

IF (OS_DARWIN AND ARCH_ARM64)
    SRCS(
        decode_arm64.s
        encode_go.go
    )
ENDIF()

END()

RECURSE(gotest)
