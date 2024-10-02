PY3TEST()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/orm/codegen/model
    yt/yt_proto/yt/orm/client/proto
)

TEST_SRCS(
    test_index_desc.py
)

END()
