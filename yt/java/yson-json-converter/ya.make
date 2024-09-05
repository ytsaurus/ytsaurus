JAVA_LIBRARY()

IF(JDK_VERSION == "")
    JDK_VERSION(11)
ENDIF()

INCLUDE(${ARCADIA_ROOT}/yt/gradle.inc)

CHECK_JAVA_DEPS(yes)

ENABLE(SOURCES_JAR)

DEFAULT_JAVA_SRCS_LAYOUT()

PEERDIR(
    yt/java/yson-tree
    contrib/java/com/fasterxml/jackson/core/jackson-databind
)

DEPENDENCY_MANAGEMENT(
    contrib/java/com/fasterxml/jackson/core/jackson-databind/2.11.3
)

END()

RECURSE_FOR_TESTS(src/test)
