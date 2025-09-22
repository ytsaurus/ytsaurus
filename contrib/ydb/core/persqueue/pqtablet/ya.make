LIBRARY()

SRCS(
    metering_sink.cpp
    pq_impl.cpp
    pq_impl_app.cpp
    pq_impl_app_sendreadset.cpp
    transaction.cpp
)



PEERDIR(
    contrib/ydb/core/persqueue/pqtablet/common
    contrib/ydb/core/persqueue/common/proxy
    contrib/ydb/core/persqueue/public/counters
    contrib/ydb/core/persqueue/pqtablet/cache
    contrib/ydb/core/persqueue/pqtablet/partition
)

END()

RECURSE(
    blob
    common
    partition
)

RECURSE_FOR_TESTS(
)
