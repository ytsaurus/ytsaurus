LIBRARY()

SRCS(
    blackbox_client_dummy.cpp
)

PEERDIR(
    yt/yql/providers/yt/lib/blackbox_client/dummy
    yt/yql/providers/yt/lib/blackbox_client/proto
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
