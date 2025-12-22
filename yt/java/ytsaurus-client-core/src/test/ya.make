JUNIT5()

JDK_VERSION(11)

SIZE(SMALL)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/java/ytsaurus-client-core
    contrib/java/org/junit/jupiter/junit-jupiter
    contrib/java/org/apache/logging/log4j/log4j-core
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl
)

# Added automatically to remove dependency on default contrib versions
DEPENDENCY_MANAGEMENT(
    contrib/java/org/junit/jupiter/junit-jupiter/5.6.1
    contrib/java/org/junit/jupiter/junit-jupiter-api/5.6.1
    contrib/java/org/apache/logging/log4j/log4j-core/2.25.0
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl/2.25.1
)

LINT(extended)
END()
