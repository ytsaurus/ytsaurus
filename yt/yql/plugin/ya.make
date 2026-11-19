LIBRARY()

SRCS(
    plugin.cpp
    config.cpp
    udf_meta.cpp
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ELSE()
    SRCS(
        plugin_ext.cpp
    )
ENDIF()

PEERDIR(
    yt/yt/core
    yt/yt/ytlib
)

END()

RECURSE(
    native
    process
    qtworker
)

RECURSE_FOR_TESTS(
    ut
)
