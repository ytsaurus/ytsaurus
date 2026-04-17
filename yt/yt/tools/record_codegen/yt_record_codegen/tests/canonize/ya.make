PY3TEST()

TEST_SRCS(
    test_canonizator.py
)

PEERDIR(
    library/python/resource
    contrib/python/pyaml
)

DEPENDS(
    yt/yt/tools/record_codegen/yt_record_codegen/bin
)

DATA(
    arcadia/yt/yt/tools/record_codegen/yt_record_codegen/tests/records
)

RESOURCE(
    record_includes.yaml record_includes.yaml
)

END()
