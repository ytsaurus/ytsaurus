GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.2.0)

SRCS(
    murmur.go
    murmur128.go
    murmur32.go
    murmur64.go
)

GO_TEST_SRCS(murmur_test.go)

END()

IF (OS_LINUX)
    RECURSE(
        testdata
        gotest
    )
ENDIF()
