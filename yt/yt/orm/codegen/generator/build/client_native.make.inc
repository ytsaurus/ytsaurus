RUN_PROGRAM(
    ${CODEGEN} render-client-native --output-dir ${OUTDIR} --proto-api-path ${PROTO_API_DIR}
    OUT
        ${OUTDIR}/client.cpp
        ${OUTDIR}/client.h
        ${OUTDIR}/connection.cpp
        ${OUTDIR}/connection.h
        ${OUTDIR}/config.cpp
        ${OUTDIR}/config.h
        ${OUTDIR}/discovery_service_proxy.cpp
        ${OUTDIR}/discovery_service_proxy.h
        ${OUTDIR}/object_service_proxy.cpp
        ${OUTDIR}/object_service_proxy.h
        ${OUTDIR}/private.h
        ${OUTDIR}/public.cpp
        ${OUTDIR}/public.h
    OUTPUT_INCLUDES
        util/folder/dirut.h
        util/folder/path.h
        util/stream/file.h
        util/string/strip.h
        util/system/env.h
        yt/yt/core/logging/log.h
        yt/yt/core/rpc/client.h
        yt/yt/orm/client/native/client.h
        yt/yt/orm/client/native/client_impl.h
        yt/yt/orm/client/native/config.h
        yt/yt/orm/client/native/connection_impl.h
        yt/yt/orm/client/native/object_service_proxy.h
        yt/yt/orm/client/native/public.h
        ${CLIENT_OBJECTS_OUTDIR}/acl.h
        ${CLIENT_OBJECTS_OUTDIR}/public.h
        ${CLIENT_OBJECTS_OUTDIR}/type.h
        ${CLIENT_OBJECTS_OUTDIR}/tags.h
        ${PROTO_API_DIR}/discovery_service.pb.h
        ${PROTO_API_DIR}/object_service.pb.h
)

PEERDIR(
    ${CLIENT_OBJECTS_DIR}
    ${PROTO_API_DIR}

    yt/yt/orm/client/native

    yt/yt/core

    util
)
