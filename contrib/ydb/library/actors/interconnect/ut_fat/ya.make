UNITTEST()

SIZE(LARGE)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/mock
    contrib/ydb/library/actors/interconnect/ut/lib
    contrib/ydb/library/actors/interconnect/ut/protos
    library/cpp/testing/unittest
    library/cpp/deprecated/atomic
)

END()
