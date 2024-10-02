PY23_LIBRARY()

STYLE_PYTHON()

PEERDIR(
    library/python/openssl
    contrib/python/paramiko
    contrib/python/six
)

PY_SRCS(
    __init__.py
)

END()

RECURSE_FOR_TESTS(
    tests
)
