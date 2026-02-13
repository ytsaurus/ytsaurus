PY3_LIBRARY()

TEST_SRCS(
    test_cypress_proxy_tracker.py
    test_grafting.py
    test_sequoia_internals.py
    test_sequoia_objects.py
    test_sequoia_compatibility.py
    test_sequoia_reconstructor.py
    test_sequoia_latency.py
)

PEERDIR(
    yt/python/yt/sequoia_tools
)

END()

RECURSE_FOR_TESTS(
    bin
)
