GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v0.17.0)

SRCS(
    airy.go
    beta.go
    betainc.go
    digamma.go
    dilog.go
    doc.go
    ell_carlson.go
    ell_complete.go
    erf.go
    gamma_inc.go
    gamma_inc_inv.go
    hypergeo.go
    mvgamma.go
    roots.go
    zeta.go
)

END()

RECURSE(
    internal
    prng
)
