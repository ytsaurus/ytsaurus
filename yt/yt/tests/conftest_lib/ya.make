PY3_LIBRARY()

NO_CHECK_IMPORTS()

PY_NAMESPACE(conftest_lib)

PY_SRCS(
    conftest.py
    conftest_queries.py
)

PEERDIR(
    yt/python/yt/environment/components/query_tracker
)

END()
