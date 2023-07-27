PY23_LIBRARY()

OWNER(
    g:yatool
    dmitko
)

TEST_SRCS(test_archive.py)

PEERDIR(
    contrib/python/six
    library/python/archive
)

END()

RECURSE(
    py2
    py3
)
