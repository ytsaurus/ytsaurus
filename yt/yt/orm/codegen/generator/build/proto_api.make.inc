GRPC()

SRCS(
    discovery_service.proto
    object_service.proto
)

IF (GEN_PROTO)
    RUN_PROGRAM(
        ${CODEGEN} render-proto-api
            --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
            --proto-api-path ${PROTO_API_DIR}
            --output-dir ${BINDIR}
        OUTPUT_INCLUDES
            ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}.proto
            yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto
            yt/yt_proto/yt/core/yson/proto/protobuf_interop.proto
            google/protobuf/duration.proto
            google/protobuf/timestamp.proto
        OUT_NOAUTO
            ${BINDIR}/discovery_service.proto
            ${BINDIR}/object_service.proto
    )
ENDIF()

PEERDIR(
    ${CLIENT_DATAMODEL_OUTDIR}

    yt/yt_proto/yt/orm/client/proto
)

IF (GO_PROTO)
    PEERDIR(
        yt/go/proto/core/yson
        yt/go/proto/core/ytree
        yt/go/proto/client/api/rpc_proxy
    )
ELSE()
    PEERDIR(
        yt/yt_proto/yt/core
        yt/yt_proto/yt/client
    )
ENDIF()
