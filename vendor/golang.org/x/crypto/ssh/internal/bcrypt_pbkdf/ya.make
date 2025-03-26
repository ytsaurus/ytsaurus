GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.32.0)

SRCS(
    bcrypt_pbkdf.go
)

GO_TEST_SRCS(bcrypt_pbkdf_test.go)

END()

RECURSE(
    gotest
)
