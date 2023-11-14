PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PY_SRCS(
    NAMESPACE yt_odin.test_helpers

    __init__.py
)

PEERDIR(
    yt/odin/lib/yt_odin/odinserver
    yt/odin/lib/yt_odin/storage

    yt/python/yt/environment/arcadia_interop
    yt/python/yt/local
    yt/python/yt/test_helpers
)

END()
