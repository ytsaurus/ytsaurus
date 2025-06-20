PY3TEST()

TEST_SRCS(
    test_schema_extraction.py
    test_type_conversion.py
)

PEERDIR(
    yt/python/yt/data/pandas
    yt/python/yt/wrapper/testlib
)

INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)

END()
