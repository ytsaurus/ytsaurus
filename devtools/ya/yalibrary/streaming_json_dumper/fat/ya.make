PY23_TEST()

TEST_SRCS(
    test_performance.py
)

PEERDIR(
    contrib/python/ujson
    devtools/ya/exts
    devtools/ya/yalibrary/streaming_json_dumper
    library/python/json
)

# graph.json.uc. See README.md
DATA(sbr://4059732606)

SIZE(LARGE)

REQUIREMENTS(
    ram:10
    cpu:all
)

TAG(
    sb:intel_e5_2660v1
    ya:fat
)

END()
