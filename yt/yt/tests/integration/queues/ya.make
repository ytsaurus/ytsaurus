PY3_LIBRARY()

# For opensource.
PY_SRCS(
    __init__.py
)

TEST_SRCS(
    test_acl.py
    test_consumer_registrations.py
    test_object_watchlist.py
    test_queue_agent.py
)

PEERDIR(
    contrib/python/pytz
    yt/python/yt/environment
)

END()

RECURSE_FOR_TESTS(
    bin
)
