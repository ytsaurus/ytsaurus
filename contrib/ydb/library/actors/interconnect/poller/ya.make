LIBRARY()

NO_WSHADOW()

IF (PROFILE_MEMORY_ALLOCATIONS)
    CFLAGS(-DPROFILE_MEMORY_ALLOCATIONS)
ENDIF()

IF (MUSL)
    # musl code for CMSG_NXTHDR is broken by this check
    CFLAGS(-Wno-sign-compare)
ENDIF()

SRCS(
    poller_actor.cpp
    poller_actor.h
    poller.h
    poller_tcp.cpp
    poller_tcp.h
    poller_tcp_unit.cpp
    poller_tcp_unit.h
    poller_tcp_unit_select.cpp
    poller_tcp_unit_select.h
)

IF (OS_LINUX)
    SRCS(
        poller_tcp_unit_epoll.cpp
        poller_tcp_unit_epoll.h
    )
ENDIF()

PEERDIR(
    contrib/libs/libc_compat
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/helpers
    contrib/ydb/library/actors/prof
    contrib/ydb/library/actors/protos
    contrib/ydb/library/actors/util
    contrib/ydb/library/actors/wilson
)

END()

