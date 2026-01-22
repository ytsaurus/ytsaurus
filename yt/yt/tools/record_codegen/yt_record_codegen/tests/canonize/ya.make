PY3TEST()

TEST_SRCS(
    test_canonizator.py
)

DEPENDS(
    yt/yt/tools/record_codegen/yt_record_codegen/bin
)

DATA(
    arcadia/yt/yt/tools/record_codegen/yt_record_codegen/tests/records
)

END()
