PY3_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/odin/lib/yt_odin/common
    yt/odin/lib/yt_odin/logging
    yt/python/client
)

PY_SRCS(
    NAMESPACE yt_odin.storage

    __init__.py
    async_storage_writer.py
    db.py
    storage.py
)

END()
