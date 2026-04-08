LIBRARY()

PEERDIR(
    yql/essentials/core/credentials
    yql/essentials/providers/common/proto
    yql/essentials/utils
    yt/yql/providers/yt/lib/tvm_client
)

END()

RECURSE(
    dummy
    full
)

IF (NOT OPENSOURCE)
    RECURSE(yandex)
ENDIF()
