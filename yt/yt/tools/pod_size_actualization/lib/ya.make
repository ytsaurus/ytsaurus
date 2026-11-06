PY3_LIBRARY()

PEERDIR(
    contrib/python/pandas
    library/python/resource
)

PY_SRCS(
    __init__.py
    subprocess_runner.py
    spare.py
)

END()
