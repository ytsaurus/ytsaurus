PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PY_SRCS(
    NAMESPACE yt.testlib
    __init__.py
    helpers.py
    test_environment.py
)

PEERDIR(
    yt/python/yt/test_helpers
    yt/python/yt/local
    yt/python/yt/environment
    yt/python/yt/environment/arcadia_interop
)

END()

