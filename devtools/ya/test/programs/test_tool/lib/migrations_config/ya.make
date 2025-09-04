PY3_LIBRARY()

PY_SRCS(
    __init__.py
)

PEERDIR(
    contrib/python/PyYAML
    contrib/python/marisa-trie
    contrib/python/six
)

END()

RECURSE_FOR_TESTS(
    tests
)
