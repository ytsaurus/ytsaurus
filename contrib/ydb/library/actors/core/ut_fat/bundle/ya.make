PROGRAM(actors_core_ut_fat)

SRCDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/core/ut_fat
)

ADDINCL(
    contrib/ydb/library/actors/core
)

PEERDIR(
    library/cpp/testing/unittest_main
    contrib/ydb/library/actors/core
)

INCLUDE(${ARCADIA_ROOT}/contrib/ydb/library/actors/core/ut_fat/sources.inc)

END()
