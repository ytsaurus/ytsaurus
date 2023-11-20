JAVA_LIBRARY(ytsaurus-client-core)

IF(JDK_VERSION == "")
    JDK_VERSION(11)
ENDIF()

LINT(extended)

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

CHECK_JAVA_DEPS(yes)

PEERDIR(
    yt/java/type-info
    yt/java/yson
    yt/java/yson-tree
    yt/yt_proto/yt/core
    yt/yt_proto/yt/formats
)

DEFAULT_JAVA_SRCS_LAYOUT()

END()

RECURSE_FOR_TESTS(src/test)
