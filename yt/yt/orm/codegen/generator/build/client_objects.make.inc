RUN_PROGRAM(
    ${CODEGEN} render-client-objects --output-dir ${OUTDIR}
    OUT
        ${OUTDIR}/acl.cpp
        ${OUTDIR}/acl.h
        ${OUTDIR}/init.cpp
        ${OUTDIR}/init.h
        ${OUTDIR}/public.cpp
        ${OUTDIR}/public.h
        ${OUTDIR}/tags.cpp
        ${OUTDIR}/tags.h
        ${OUTDIR}/type.cpp
        ${OUTDIR}/type.h
    OUTPUT_INCLUDES
        yt/yt/orm/client/objects/acl.h
        yt/yt/orm/client/objects/public.h
        yt/yt/orm/client/objects/registry.h
        yt/yt/orm/client/objects/tags.h
        yt/yt/orm/client/objects/type.h
        mutex
)

PEERDIR(
    yt/yt/orm/client/objects

    yt/yt/core
)
