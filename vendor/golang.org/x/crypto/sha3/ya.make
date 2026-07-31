GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.53.0)

SRCS(
    hashes.go
    legacy_hash.go
    legacy_keccakf.go
    shake.go
)

GO_TEST_SRCS(sha3_test.go)

END()

RECURSE(
    gotest
)
