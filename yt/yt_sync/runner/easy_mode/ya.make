PY3_LIBRARY()

STYLE_PYTHON()

PY_SRCS(
    __init__.py
    make_description.py
)

PEERDIR(
    yt/yt_sync/runner/builtin_presets
    yt/yt_sync/runner/core

    yt/yt_sync/core/spec_merger
)

END()

RECURSE_FOR_TESTS(ut)
