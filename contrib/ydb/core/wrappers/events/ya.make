LIBRARY()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_WRAPPER
    )
ELSE()
    SRCS(
        common.cpp
        object_exists.cpp
        get_object.cpp
        s3_out.cpp
        abstract.cpp
    )
    PEERDIR(
        contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
        contrib/libs/curl
        contrib/ydb/core/base
        contrib/ydb/core/protos
        contrib/ydb/library/actors/core
    )
ENDIF()

END()
