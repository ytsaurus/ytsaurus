LIBRARY()

PEERDIR(
    yql/essentials/core/credentials
    yql/essentials/utils
    yt/yql/providers/yt/lib/tvm_client
)

END()

RECURSE(
    dummy
    full
    proto
)

IF (NOT OPENSOURCE)
    RECURSE(yandex)
ENDIF()
