PY23_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

IF (PYTHON2)
    PEERDIR(yt/python_py2/yt/operations_archive)
ELSE()
    NO_LINT()

    PY_SRCS(
        NAMESPACE yt.operations_archive

        __init__.py
        clear_operations.py
        queues.py
    )
ENDIF()

END()
