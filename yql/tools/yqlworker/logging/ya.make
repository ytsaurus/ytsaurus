YQL_LIBRARY()

PEERDIR(
    yql/essentials/utils/log/proto
)

IF (NOT OPENSOURCE)
    SRCS(
        provider_load.cpp
    )
    PEERDIR(
        yql/library/logger/ua_backend_creator
    )
ELSE()
    SRCS(
        provider_load_opensource.cpp
    )
ENDIF()

END()
