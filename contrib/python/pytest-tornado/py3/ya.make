# Generated by devtools/yamaker (pypi).

PY3_LIBRARY()

VERSION(0.8.1)

LICENSE(Apache-2.0)

PEERDIR(
    contrib/python/pytest
    contrib/python/setuptools
)

NO_LINT()

PY_SRCS(
    TOP_LEVEL
    pytest_tornado/__init__.py
    pytest_tornado/plugin.py
)

RESOURCE_FILES(
    PREFIX contrib/python/pytest-tornado/py3/
    .dist-info/METADATA
    .dist-info/entry_points.txt
    .dist-info/top_level.txt
)

END()

RECURSE_FOR_TESTS(
    tests
)
