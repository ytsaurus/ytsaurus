PY3_LIBRARY()

PY_SRCS(__init__.py)

PEERDIR(
    yt/yt/flow/library/python/client/state_unpack/bindings
    yt/python/yt/wrapper
)

END()

RECURSE_FOR_TESTS(tests)
