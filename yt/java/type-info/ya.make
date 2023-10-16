JAVA_LIBRARY(ytsaurus-type-info)

IF(JDK_VERSION == "")
    JDK_VERSION(11)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

CHECK_JAVA_DEPS(yes)

PEERDIR(
    yt/java/annotations
    yt/java/yson
)

JAVA_SRCS(SRCDIR src/main/java **/*.java)

END()

RECURSE_FOR_TESTS(
    src/test
)
