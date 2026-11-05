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

    # Programs get a path-derived cmake export target name so identically-named modules
    # (e.g. PROGRAM(pipeline), G_BENCHMARK) don't collide in the export. A binary that needs
    # a specific target name (e.g. deployables under bin/ and tools/) sets
    # FLOW_CMAKE_TARGET_NAME before including this file.
    IF (MODULE_TYPE == "PROGRAM")
        IF (FLOW_CMAKE_TARGET_NAME)
            CMAKE_EXPORTED_TARGET_NAME($FLOW_CMAKE_TARGET_NAME)
        ELSE()
            CMAKE_EXPORTED_TARGET_NAME_FROM_PATH(yt)
        ENDIF()
    ENDIF()
ENDIF()
