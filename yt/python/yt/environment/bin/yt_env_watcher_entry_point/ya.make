PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/environment/bin/yt_env_watcher_entry_point)
ELSE()
    NO_CHECK_IMPORTS(
        __yt_env_watcher_entry_point__
    )

    COPY_FILE(
        ../yt_env_watcher
        yt_env_watcher.py
    )

    PY_SRCS(
        TOP_LEVEL

        __yt_env_watcher_entry_point__.py
        yt_env_watcher.py
    )
ENDIF()

END()
