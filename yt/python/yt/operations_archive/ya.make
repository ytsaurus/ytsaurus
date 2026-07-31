PY3_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

NO_LINT()

PY_SRCS(
    NAMESPACE yt.operations_archive

    __init__.py
    clear_operations.py
    queues.py
)

END()
