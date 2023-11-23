
JTEST()

JDK_VERSION(11)

SIZE(SMALL)

DEFAULT_JUNIT_JAVA_SRCS_LAYOUT()

INCLUDE(${ARCADIA_ROOT}/yt/opensource.inc)

PEERDIR(
    yt/java/yson-tree
    contrib/java/junit/junit
    contrib/java/org/apache/logging/log4j/log4j-core
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl
)

# Added automatically to remove dependency on default contrib versions
DEPENDENCY_MANAGEMENT(
    contrib/java/junit/junit/4.13
    contrib/java/org/apache/logging/log4j/log4j-core/2.13.1
    contrib/java/org/apache/logging/log4j/log4j-slf4j-impl/2.13.1
)

LINT(extended)

END()
