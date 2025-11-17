LIBRARY()

IF (OS_LINUX)

    SRCS(
        cq_actor.cpp
    )

    PEERDIR(
        contrib/libs/ibdrv
        contrib/libs/protobuf
        contrib/ydb/library/actors/core
        contrib/ydb/library/actors/protos
        contrib/ydb/library/actors/util
    )

ELSE()

    SRCS(
        cq_actor_dummy.cpp
    )

    PEERDIR(
        contrib/libs/protobuf
        contrib/ydb/library/actors/core
        contrib/ydb/library/actors/protos
        contrib/ydb/library/actors/util
    )

ENDIF()

END()
