PY3_LIBRARY()

PEERDIR(
    yt/python/yt/wrapper
    contrib/python/py4j
    contrib/python/pyarrow
    contrib/python/PyYAML
)

PY_SRCS(
    TOP_LEVEL
    spyt/__init__.py
    spyt/client.py
    spyt/conf.py
    spyt/dependency_utils.py
    spyt/enabler.py
    spyt/standalone.py
    spyt/utils.py
    spyt/version.py
)

END()
