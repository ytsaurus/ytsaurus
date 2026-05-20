GTEST(unittester-transaction-supervisor)

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

ALLOCATOR(TCMALLOC)

SRCS(
    strong_oredring_manager_ut.cpp
)

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/yt/server/lib/transaction_supervisor
)

SIZE(MEDIUM)

END()
