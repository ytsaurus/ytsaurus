INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

IF (NOT OPENSOURCE)
    CXXFLAGS(-DYT_INTERNAL_FLOW)
ENDIF()

# The Yandex-internal part of flow (yt/yt/flow/yandex) is cut from the opensource export
# and depends on internal-only trees, so it skips the opensource dependency restriction
# and cmake export target uniquification below.
IF (NOT MODDIR MATCHES "/yandex/")
    CHECK_DEPENDENT_DIRS(ALLOW_ONLY
        PEERDIR
            yt/
            library/
            contrib/
            util/
        ALL
            build/
            certs/
            infra/
            tools/
    )

    # Give flow program/test binaries a path-derived cmake export target name so
    # identically-named modules (e.g. PROGRAM(pipeline), G_BENCHMARK) don't collide.
    IF (MODULE_TYPE == "PROGRAM")
        CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(yt)
    ENDIF()
ENDIF()
