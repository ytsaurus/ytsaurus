IF (GEN_PROTO)
    RUN_PROGRAM(
        ${CODEGEN} render-data-model-proto
        IN
            ${CLIENT_DATAMODEL_DIR}/files.proto
            yt/yt_proto/yt/orm/data_model/files.proto
        OUTPUT_INCLUDES
            ${CLIENT_DATAMODEL_DIR}/files.proto
            yt/yt_proto/yt/orm/data_model/files.proto
            yt/yt_proto/yt/core/ytree/proto/attributes.proto
        STDOUT_NOAUTO ${CLIENT_DATAMODEL_FILENAME}.proto
    )
ENDIF()

SRCS(${CLIENT_DATAMODEL_FILENAME}.proto)

PEERDIR(
    ${CLIENT_DATAMODEL_DIR}

    yt/yt_proto/yt/orm/client/proto
    yt/yt_proto/yt/orm/data_model
)

IF (GO_PROTO)
    PEERDIR(yt/go/proto/core/yson)
    PEERDIR(yt/go/proto/core/ytree)
ELSE()
    PEERDIR(yt/yt_proto/yt/core)
ENDIF()
