LIBRARY()

SRCS(
    readproxy.cpp
)



PEERDIR(
    contrib/ydb/core/persqueue/events
    contrib/ydb/core/persqueue/common
)

END()

RECURSE_FOR_TESTS(
)
