GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.17.0)

SRCS(
    doc.go
    pca_cca.go
    roc.go
    stat.go
    statmat.go
)

END()

RECURSE(
    card
    combin
    distmat
    distmv
    distuv
    mds
    samplemv
    sampleuv
    spatial
)
