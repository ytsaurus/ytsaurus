GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.17.0)

SRCS(
    doc.go
    lapack.go
)

END()

RECURSE(
    gonum
    lapack64
    testlapack
)
