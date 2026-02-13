LIBRARY()

SRCS(
    account_read_quoter.cpp
    quota_tracker.cpp
    quoter_base.cpp
    read_quoter.cpp
    write_quoter.cpp
)



PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/common
    contrib/ydb/core/persqueue/public/counters
    contrib/ydb/core/persqueue/pqtablet/common
    contrib/ydb/core/quoter/public
)

END()

RECURSE_FOR_TESTS(
    ut
)
