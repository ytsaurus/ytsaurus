LIBRARY()

SRCS(
    yt_token_resolver.cpp
)

PEERDIR(
    yt/yql/library/token_resolver/proto
    yt/yql/providers/yt/lib/yt_token_resolver
)

IF (OS_LINUX)
    SRCS(
        yql_agent_token_resolver.cpp
    )

    PEERDIR(
        yql/essentials/core/credentials
        yql/essentials/utils
        yql/essentials/utils/log
        yt/yt/core
        yt/yt/ytlib
    )
ENDIF()

END()

RECURSE(
    proto
)
