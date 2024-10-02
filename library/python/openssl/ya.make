PY23_LIBRARY()

NO_WSHADOW()

PEERDIR(
    contrib/libs/openssl
)

PY_SRCS(
    TOP_LEVEL
    openssl.pyx
)

END()
