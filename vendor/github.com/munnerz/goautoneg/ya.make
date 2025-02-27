GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.0.0-20191010083416-a7dc8b61c822)

SRCS(
    autoneg.go
)

GO_TEST_SRCS(autoneg_test.go)

END()

RECURSE(
    gotest
)
