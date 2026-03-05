GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.17.0)

SRCS(
    blas.go
    doc.go
)

END()

RECURSE(
    blas32
    blas64
    cblas128
    cblas64
    gonum
    testblas
)
