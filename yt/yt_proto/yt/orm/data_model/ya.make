PROTO_LIBRARY()

PROTO_NAMESPACE(yt)

PY_NAMESPACE(yt_proto.yt.orm.data_model)

SET_APPEND(PROTO_FILES
    access_control.proto
    controls.proto
    finalizers.proto
    generic.proto
    geo_point.proto
    group.proto
    semaphore_set.proto
    semaphore.proto
    schema.proto
    user.proto
    tags.proto
    watch_log_consumer.proto
)

SRCS(${PROTO_FILES})

LIST_PROTO(${PROTO_FILES})

PEERDIR(
    yt/yt_proto/yt/orm/client/proto
)

IF (GO_PROTO)
    PEERDIR(
        yt/go/proto/core/yson
        yt/go/proto/core/ytree
    )
ELSE()
    PEERDIR(yt/yt_proto/yt/core)
ENDIF()

END()
