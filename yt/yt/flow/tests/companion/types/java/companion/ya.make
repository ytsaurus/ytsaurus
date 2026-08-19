JAVA_PROGRAM(companion_bin)

JDK_VERSION(21)
WITH_JDK()

PEERDIR(
    yt/java/flow/flow-server
    yt/java/flow/flow-runner

    contrib/java/org/apache/logging/log4j/log4j-slf4j2-impl
    contrib/java/javax/persistence/persistence-api/1.0
)

DEFAULT_JAVA_SRCS_LAYOUT()

DEPENDENCY_MANAGEMENT(
    contrib/java/org/apache/logging/log4j/log4j-slf4j2-impl/2.25.1
)

LINT(base)

END()
