PY23_LIBRARY()

OWNER(pg)

PEERDIR(
    contrib/python/six
    library/python/resource
    library/python/json
)

TEST_SRCS(
    test_compat.py
    test_file.py
)

# https://github.com/miloyip/nativejson-benchmark/tree/master/data
FROM_SANDBOX(FILE 266018317 OUT_NOAUTO data.tar.gz)

# https://github.com/nst/JSONTestSuite/tree/master/test_parsing
FROM_SANDBOX(FILE 268482613 OUT_NOAUTO test_parsing.tar.gz)

RESOURCE(
    data.tar.gz /data.tar.gz
    test_parsing.tar.gz /test_parsing.tar.gz
)

END()
