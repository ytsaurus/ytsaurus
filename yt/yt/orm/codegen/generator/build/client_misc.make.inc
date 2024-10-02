RUN_PROGRAM(
    ${CODEGEN} render-client-misc
        --client-schema-path ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}
        --output-dir ${OUTDIR}
    OUT
        ${OUTDIR}/enums.cpp
        ${OUTDIR}/enums.h
        ${OUTDIR}/index_helpers.cpp
        ${OUTDIR}/index_helpers.h
        ${OUTDIR}/schema_transitive.h
        ${OUTDIR}/traits.cpp
        ${OUTDIR}/traits.h
    OUTPUT_INCLUDES
        util/generic/hash.h
        utility
        library/cpp/yt/misc/enum.h
        yt/yt/orm/library/mpl/types.h
        yt/yt/orm/client/objects/key.h
        yt/yt/orm/client/misc/protobuf_helpers.h
        yt/yt/orm/client/objects/public.h
        ${CLIENT_DATAMODEL_OUTDIR}/${CLIENT_DATAMODEL_FILENAME}.pb.h
)

PEERDIR(
    ${CLIENT_DATAMODEL_OUTDIR}

    yt/yt/orm/client/objects
    yt/yt/orm/library/mpl

    library/cpp/yt/misc
)
