EXECTEST()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/codegen/config.inc)

RUN(
    NAME proto_api
    codegen render-proto-api
        --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
        --proto-api-path ${PROTO_API_DIR}
        --output-dir ${TEST_OUT_ROOT}/proto_api
    CANONIZE_DIR_LOCALLY ${TEST_OUT_ROOT}/proto_api
)

RUN(
    NAME data_model_proto
    codegen render-data-model-proto --output-path ${TEST_OUT_ROOT}/client_schema.proto
    CANONIZE_LOCALLY ${TEST_OUT_ROOT}/client_schema.proto
)

RUN(
    NAME client_misc
    codegen render-client-misc
        --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
        --output-dir ${TEST_OUT_ROOT}/client_misc
    CANONIZE_DIR_LOCALLY ${TEST_OUT_ROOT}/client_misc
)

RUN(
    NAME server_proto
    codegen render-server-proto
        --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
        --output-dir ${TEST_OUT_ROOT}/server_proto
    CANONIZE_DIR_LOCALLY ${TEST_OUT_ROOT}/server_proto
)

RUN(
    NAME objects_lib
    codegen render-objects-lib
        --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
        --output-dir ${TEST_OUT_ROOT}/objects_lib
        --shard-count 2
    CANONIZE_DIR_LOCALLY ${TEST_OUT_ROOT}/objects_lib
)

RUN(
    NAME yt_schema
    codegen render-yt-schema --output-path ${TEST_OUT_ROOT}/schema.py
    CANONIZE_LOCALLY ${TEST_OUT_ROOT}/schema.py
)

RUN(
    NAME client_native
    codegen render-client-native
        --proto-api-path ${PROTO_API_DIR}
        --output-dir ${TEST_OUT_ROOT}/client_native
    CANONIZE_DIR_LOCALLY ${TEST_OUT_ROOT}/client_native
)

RUN(
    NAME client_objects
    codegen render-client-objects --output-dir ${TEST_OUT_ROOT}/client_objects
    CANONIZE_DIR_LOCALLY ${TEST_OUT_ROOT}/client_objects
)

RUN(
    NAME main_cpp
    codegen render-main-cpp --output-path ${TEST_OUT_ROOT}/main.cpp
    CANONIZE_LOCALLY ${TEST_OUT_ROOT}/main.cpp
)

RUN(
    NAME program_lib
    codegen render-program-lib --output-dir ${TEST_OUT_ROOT}/program_lib
    CANONIZE_DIR_LOCALLY ${TEST_OUT_ROOT}/program_lib
)

FORK_SUBTESTS()

DEPENDS(
    yt/yt/orm/example/codegen
)

END()
