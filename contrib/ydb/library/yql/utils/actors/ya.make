LIBRARY()

SRCS(
    rich_actor.cpp
    http_sender_actor.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    yql/essentials/public/types
    yql/essentials/public/udf
    contrib/ydb/library/yql/providers/solomon/proto
)

END()

IF (NOT OPENSOURCE OR OPENSOURCE_PROJECT == "ydb")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
