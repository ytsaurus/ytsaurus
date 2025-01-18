PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
)

PEERDIR(
    yt/yt_sync/runner/core
    yt/yt_sync/runner/easy_mode
)

END()

RECURSE(
    builtin_presets
    core
    easy_mode
)
