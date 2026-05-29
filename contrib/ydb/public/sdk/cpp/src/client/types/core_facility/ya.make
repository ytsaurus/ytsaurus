LIBRARY()

SRCS(
    simple_core_facility.cpp
    simple_core_facility.h
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/types/status
    contrib/ydb/public/sdk/cpp/src/library/time
)

END()
