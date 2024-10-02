LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/ya_cpp.make.inc)
INCLUDE(${ARCADIA_ROOT}/yt/yt/orm/example/codegen/config.inc)

RUN_PROGRAM(
    yt/yt/orm/example/codegen render-objects-lib
        --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
        --output-dir ${BINDIR}/autogen
        --shard-count 8
    OUT
        ${BINDIR}/autogen/config.cpp
        ${BINDIR}/autogen/config.h
        ${BINDIR}/autogen/db_schema.cpp
        ${BINDIR}/autogen/db_schema.h
        ${BINDIR}/autogen/dynamic_config_manager.cpp
        ${BINDIR}/autogen/dynamic_config_manager.h
        ${BINDIR}/autogen/object_detail.cpp
        ${BINDIR}/autogen/object_detail.h
        ${BINDIR}/autogen/object_manager.cpp
        ${BINDIR}/autogen/object_manager.h
        ${BINDIR}/autogen/objects.h
        ${BINDIR}/autogen/objects_0.cpp
        ${BINDIR}/autogen/objects_1.cpp
        ${BINDIR}/autogen/objects_2.cpp
        ${BINDIR}/autogen/objects_3.cpp
        ${BINDIR}/autogen/objects_4.cpp
        ${BINDIR}/autogen/objects_5.cpp
        ${BINDIR}/autogen/objects_6.cpp
        ${BINDIR}/autogen/objects_7.cpp
        ${BINDIR}/autogen/public.cpp
        ${BINDIR}/autogen/public.h
        ${BINDIR}/autogen/type_handlers.h
        ${BINDIR}/autogen/type_handlers.cpp
        ${BINDIR}/autogen/type_handler_impls.h
        ${BINDIR}/autogen/type_handler_impls_0.cpp
        ${BINDIR}/autogen/type_handler_impls_1.cpp
        ${BINDIR}/autogen/type_handler_impls_2.cpp
        ${BINDIR}/autogen/type_handler_impls_3.cpp
        ${BINDIR}/autogen/type_handler_impls_4.cpp
        ${BINDIR}/autogen/type_handler_impls_5.cpp
        ${BINDIR}/autogen/type_handler_impls_6.cpp
        ${BINDIR}/autogen/type_handler_impls_7.cpp
    OUTPUT_INCLUDES
        memory
        vector

        util/generic/string.h
        util/system/types.h

        library/cpp/yt/memory/intrusive_ptr.h
        library/cpp/yt/memory/ref_counted.h
        library/cpp/yt/memory/ref_tracked.h

        yt/yt/core/yson/public.h

        yt/yt/library/dynamic_config/config.h
        yt/yt/library/dynamic_config/dynamic_config_manager.h
        yt/yt/library/dynamic_config/public.h

        yt/yt/orm/server/access_control/access_control_manager.h
        yt/yt/orm/server/access_control/public.h
        yt/yt/orm/server/access_control/config.h
        yt/yt/orm/server/master/bootstrap.h
        yt/yt/orm/server/master/config.h
        yt/yt/orm/server/master/public.h
        yt/yt/orm/server/master/yt_connector.h
        yt/yt/orm/server/objects/data_model_object.h
        yt/yt/orm/server/objects/db_config.h
        yt/yt/orm/server/objects/group_type_handler_detail.h
        yt/yt/orm/server/objects/helpers.h
        yt/yt/orm/server/objects/object_manager.h
        yt/yt/orm/server/objects/public.h
        yt/yt/orm/server/objects/schema_type_handler_detail.h
        yt/yt/orm/server/objects/semaphore_detail.h
        yt/yt/orm/server/objects/semaphore_set_detail.h
        yt/yt/orm/server/objects/semaphore_set_type_handler_detail.h
        yt/yt/orm/server/objects/semaphore_type_handler_detail.h
        yt/yt/orm/server/objects/subject_type_handler_detail.h
        yt/yt/orm/server/objects/build_tags.h
        yt/yt/orm/server/objects/type_handler.h
        yt/yt/orm/server/objects/type_handler_detail.h
        yt/yt/orm/server/objects/user_type_handler_detail.h
        yt/yt/orm/server/objects/watch_log.h
        yt/yt/orm/server/objects/watch_log_consumer_type_handler_detail.h

        yt/yt/orm/example/client/misc/autogen/enums.h
        yt/yt/orm/example/client/proto/data_model/autogen/schema.pb.h

        yt/yt/orm/example/server/proto/autogen/etc.pb.h

        yt/yt/orm/example/plugins/server/library/custom_base_type_handler.h
        yt/yt/orm/example/plugins/server/library/mother_ship.h
)

RUN_PROGRAM(
    yt/yt/orm/example/codegen render-program-lib --output-dir ${BINDIR}/autogen
    OUT
        ${BINDIR}/autogen/program.cpp
        ${BINDIR}/autogen/program.h
    OUTPUT_INCLUDES
        yt/yt/orm/example/server/library/bootstrap.h

        yt/yt/orm/example/client/objects/autogen/init.h

        yt/yt/orm/server/program/program.h

        yt/yt/core/ytree/public.h
)

SRCS(
    bootstrap.cpp
    ../../plugins/server/library/cat_type_handler.cpp
    ../../plugins/server/library/employer_type_handler.cpp
    ../../plugins/server/library/editor_type_handler.cpp
    ../../plugins/server/library/genre_type_handler.cpp
    ../../plugins/server/library/manualid_type_handler.cpp
    ../../plugins/server/library/mother_ship_type_handler.cpp
    ../../plugins/server/library/mother_ship.cpp
    ../../plugins/server/library/publisher_type_handler.cpp
    ../../plugins/server/library/custom_base_type_handler.cpp
)

PEERDIR(
    yt/yt/orm/example/server/proto/autogen

    yt/yt/orm/example/client/misc
    yt/yt/orm/example/client/native
    yt/yt/orm/example/client/proto/api

    yt/yt/orm/example/build

    yt/yt/orm/client/objects
    yt/yt/orm/server
    yt/yt/orm/server/program

    yt/yt/library/auth_server
    yt/yt/library/dynamic_config
    yt/yt/library/profiling
    yt/yt/library/program

    yt/yt/core
    yt/yt/core/http
    yt/yt/core/https
    yt/yt/core/rpc/grpc
    yt/yt/core/rpc/http

    library/cpp/protobuf/interop
    library/cpp/string_utils/base64
)

END()
