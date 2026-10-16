GO_LIBRARY()

LICENSE(Apache-2.0)

VERSION(v2.6.0)

SRCS(
    constant.go
    testutil.go
    wycheproofutil.go
)

END()

RECURSE(
    hybrid
)
