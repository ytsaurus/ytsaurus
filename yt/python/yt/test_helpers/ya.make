PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/test_helpers)
ELSE()
    PY_SRCS(
        NAMESPACE yt.test_helpers

        __init__.py
        authors.py
        cleanup.py
        filter_by_category.py
        fork_class.py
        job_events.py
        profiler.py
        set_timeouts.py
    )

    PEERDIR(
        yt/python/yt
        yt/python/yt/wrapper

        contrib/python/pytest
        contrib/python/pytest-timeout
    )
ENDIF()

END()
