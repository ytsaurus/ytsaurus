PROTO_LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/codegen/config.inc)

GRPC()

IF (GEN_PROTO)
    RUN_PROGRAM(
        yt/yt/orm/example/codegen render-server-proto
            --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
            --output-dir ${BINDIR}
        IN
            yt/yt/orm/example/client/proto/data_model/files.proto
            yt/yt_proto/yt/orm/data_model/files.proto
        OUTPUT_INCLUDES
            yt/yt/orm/example/client/proto/data_model/files.proto
            yt/yt/orm/example/client/proto/data_model/autogen/schema.proto
            yt/yt_proto/yt/orm/data_model/files.proto
            yt/yt_proto/yt/core/ytree/proto/attributes.proto
        OUT_NOAUTO
            continuation_token.proto
            etc.proto
    )
ENDIF()

SRCS(
    continuation_token.proto
    etc.proto
)

PEERDIR(
    yt/yt/orm/example/client/proto/data_model
    yt/yt/orm/example/client/proto/data_model/autogen

    yt/yt_proto/yt/orm/client/proto
    yt/yt_proto/yt/orm/data_model
)

IF (GO_PROTO)
    PEERDIR(yt/go/proto/core/yson)
    PEERDIR(yt/go/proto/core/ytree)
ELSE()
    PEERDIR(yt/yt_proto/yt/core)
ENDIF()

END()
